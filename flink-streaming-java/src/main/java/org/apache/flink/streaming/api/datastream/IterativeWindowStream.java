package org.apache.flink.streaming.api.datastream;


import org.apache.flink.annotation.Public;
import org.apache.flink.streaming.api.functions.windowing.CoWindowTerminateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.transformations.CoFeedbackTransformation;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.EvictingWindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.TwoWindowTerminateOperator;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows.of;

@Public
public class IterativeWindowStream<IN,IN_W extends Window,F,K,R,S> {

	private StreamExecutionEnvironment environment;
	CoWindowTerminateFunction<IN,F,S,R,K,IN_W> coWinTerm;
	private CoFeedbackTransformation<R> coFeedbackTransformation;

	private DataStream<S> outStream;

	public IterativeWindowStream(WindowedStream<IN, K,IN_W> input,
								 CoWindowTerminateFunction<IN,F,S,R,K,IN_W> coWinTerm,
								 FeedbackBuilder<R> feedbackBuilder,
								 TypeInformation<R> feedbackType,
								 long waitTime) throws Exception {
		environment = input.getExecutionEnvironment();
		WindowedStream<IN, K, IN_W> windowedStream1 = input;

		// create feedback edge
		coFeedbackTransformation = new CoFeedbackTransformation(input.getInput().getParallelism(),
			feedbackType, waitTime);

		// create feedback source
		KeyedStream<F,K> feedbackSourceStream = feedbackBuilder.feedback(new DataStream<R>(environment, coFeedbackTransformation));
		WindowAssigner assinger = TumblingEventTimeWindows.of(Time.milliseconds(1));
		WindowedStream<F, K, TimeWindow> windowedStream2 = new WindowedStream<>(feedbackSourceStream, assinger);

		// create feedback sink
		Tuple2<DataStream<R>, DataStream<S>> streams = applyCoWinTerm(coWinTerm, windowedStream1, windowedStream2);
		coFeedbackTransformation.addFeedbackEdge(streams.f0.getTransformation());
		outStream = streams.f1;
	}

	public Tuple2<DataStream<R>, DataStream<S>> applyCoWinTerm(CoWindowTerminateFunction coWinTerm,
															   WindowedStream<IN, K, IN_W> windowedStream1,
															   WindowedStream<F, K, TimeWindow> windowedStream2) throws Exception {
		TwoInputTransformation<IN,F,Either<R,S>> transformation = getTransformation(coWinTerm,windowedStream1,windowedStream2);
		DataStream<Either<R,S>> combinedOutputStream = new SingleOutputStreamOperator<Either<R,S>>(environment, transformation);

		SplitStream<Either<R,S>> splitStream = combinedOutputStream.split(new OutputSelector<Either<R, S>>() {
			@Override
			public Iterable<String> select(Either<R, S> value) {
				return value.isLeft() ? Collections.singletonList("FEEDBACK") : Collections.singletonList("FORWARD");
			}
		});
		DataStream<R> feedbackStream = splitStream.select("FEEDBACK").map(new MapFunction<Either<R, S>, R>() {
			@Override
			public R map(Either<R, S> value) throws Exception {
				return value.left();
			}
		});
		DataStream<S> forwardStream = splitStream.select("FORWARD").map(new MapFunction<Either<R, S>, S>() {
			@Override
			public S map(Either<R, S> value) throws Exception {
				return value.right();
			}
		});

		return new Tuple2(feedbackStream, forwardStream);
	}

	public DataStream<S> loop() throws Exception {
		return outStream;
	}

	// TODO does the (required) final do harm?
	public <R,S> TwoInputTransformation<IN,F,Either<R,S>> getTransformation(
		final CoWindowTerminateFunction<IN,F,S,R,K,IN_W> coWinTerm,
		WindowedStream<IN, K, IN_W> windowedStream1,
		WindowedStream<F, K, TimeWindow> windowedStream2) throws Exception {

		//TODO: is this correct?!
		TypeInformation<Either<R,S>> outTypeInfo = TypeExtractor.getBinaryOperatorReturnType(coWinTerm,
			CoWindowTerminateFunction.class, false, true, windowedStream1.getInputType(),
			windowedStream2.getInputType(), Utils.getCallLocationName(), true);

		Tuple2<String, WindowOperator> op1 =
			getWindowOperator(windowedStream1, new WindowFunction<IN, Either<R,S>, K, IN_W>() {
				public void apply(K key, IN_W window, Iterable<IN> input, Collector<Either<R,S>> out) throws Exception{
					coWinTerm.apply1(key,window,input,out);
				}
			}, outTypeInfo);
		Tuple2<String, WindowOperator> op2 =
			getWindowOperator(windowedStream2, new WindowFunction<F, Either<R,S>, K, TimeWindow>() {
				public void apply(K key, TimeWindow window, Iterable<F> input, Collector<Either<R,S>> out) throws Exception {
					coWinTerm.apply2(key,window,input,out);
				}
			}, outTypeInfo);

		String opName = "TwoWindowTerminate(" + op1.f0 + ", " + op2.f0 + ")";
		TwoWindowTerminateOperator combinedOperator = new TwoWindowTerminateOperator(op1.f1, op2.f1, coWinTerm);
		TwoInputTransformation<IN,F,Either<R,S>> transformation = new TwoInputTransformation(
			windowedStream1.getInput().getTransformation(),
			windowedStream2.getInput().getTransformation(),
			opName,
			combinedOperator,
			outTypeInfo,
			windowedStream1.getInput().getParallelism()
		);
		return transformation;
	}

	// rougly the same like WindowedStream#apply(Windowfunction, resultType) but for two window inputs
	public <R, T, WIN extends Window> Tuple2<String, WindowOperator>
			getWindowOperator(WindowedStream<T, K, WIN> windowedStream,
						  WindowFunction<T, R, K, WIN> function,
						  TypeInformation<R> resultType) {
		KeyedStream<T, K> input = windowedStream.getInput();
		WindowAssigner windowAssigner = windowedStream.getWindowAssigner();
		StreamExecutionEnvironment environment = windowedStream.getExecutionEnvironment();

		//clean the closure
		function = input.getExecutionEnvironment().clean(function);

		String callLocation = Utils.getCallLocationName();
		String udfName = "WindowedStream." + callLocation;

		String opName;
		KeySelector<T, K> keySel = input.getKeySelector();

		WindowOperator<K, T, Iterable<T>, R, WIN> operator;

		if (windowedStream.evictor != null) {
			@SuppressWarnings({"unchecked", "rawtypes"})
			TypeSerializer<StreamRecord<T>> streamRecordSerializer =
				(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(input.getType().createSerializer(environment.getConfig()));

			ListStateDescriptor<StreamRecord<T>> stateDesc =
				new ListStateDescriptor<>("window-contents", streamRecordSerializer);

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + windowedStream.trigger + ", " + windowedStream.evictor + ", " + udfName + ")";

			operator =
				new EvictingWindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(environment.getConfig()),
					keySel,
					input.getKeyType().createSerializer(environment.getConfig()),
					stateDesc,
					new InternalIterableWindowFunction<>(function),
					windowedStream.trigger,
					windowedStream.evictor,
					windowedStream.allowedLateness);

		} else {
			ListStateDescriptor<T> stateDesc = new ListStateDescriptor<>("window-contents",
				input.getType().createSerializer(environment.getConfig()));

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + windowedStream.trigger + ", " + udfName + ")";

			operator =
				new WindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(environment.getConfig()),
					keySel,
					input.getKeyType().createSerializer(environment.getConfig()),
					stateDesc,
					new InternalIterableWindowFunction<>(function),
					windowedStream.trigger,
					windowedStream.allowedLateness);
		}

		return new Tuple2(opName, operator);
	}
}
