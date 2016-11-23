package org.apache.flink.streaming.api.datastream;


import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.TerminationFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.CoFeedbackTransformation;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.types.Either;
import org.mortbay.util.SingletonList;

import java.util.Collection;
import java.util.Collections;

@Public
public class IterativeWindowStream<IN,IN_W extends Window,F,KEY> {

	private StreamExecutionEnvironment environment;
	private WindowedStream<IN, KEY, IN_W> windowedStream1;
	private WindowedStream<F, KEY, TimeWindow> windowedStream2;

	// TODO: Was damit machen???
	private CoFeedbackTransformation<F> coFeedbackTransformation;

	public IterativeWindowStream(WindowedStream<IN,KEY,IN_W> input,
								 TypeInformation<F> feedbackType,
								 KeySelector<F, KEY> feedbackKeySelector,
								 long waitTime) {
		environment = input.getExecutionEnvironment();
		windowedStream1 = input;

		DataStream feedBack = new DataStream(environment,
			new CoFeedbackTransformation(input.getInput().getParallelism(), feedbackType, waitTime));
		KeyedStream feedBackKeyed = new KeyedStream(feedBack, feedbackKeySelector);
		WindowAssigner assinger = TumblingEventTimeWindows.of(Time.milliseconds(1));
		windowedStream2 = new WindowedStream<>(feedBackKeyed, assinger);
	}

	/**
	 *FIXME
	 * 1) Keep one UDF, Either WindowLoopFunction or IterativeWindowFunction...
	 * 2) Implement the TerminationListener on that function
	 * 3) See the ReduceApplyWindowFunction on how you can apply a reduce function on the contents of a window but
	 * also check the CoFlatMapFunction which is a binary operator (with common state) which is what we want to combine here
	 */
	public <R, S> Tuple2<DataStream<R>, DataStream<S>> reduce(ReduceFunction reduce1, ReduceFunction reduce2) {
		// TODO: create a Transformation that produces two (differently typed) outputs in StreamGraphGenerator
		// somehow have one transformation that includes a TwoWindowTerminateOperator and can be used in two streams
		DataStream<R> output = new DataStream<>(environment, /* ?? */);
		DataStream<S> feedback = new DataStream(environment, /* ?? */);

		//SPLIT LOGIC
//		DataStream<Either<R,S>> outStream;
//		SplitStream<Either<R,S>> splitStream = outStream.split(new OutputSelector<Either<R, S>>() {
//			@Override
//			public Iterable<String> select(Either<R, S> value) {
//				return value.isLeft() ? Collections.singletonList("FEEDBACK") : Collections.singletonList("FORWARD");
//			}
//		});
//		DataStream<R> feedbackStream = splitStream.select("FEEDBACK").map(new MapFunction<Either<R, S>, R>() {
//			@Override
//			public R map(Either<R, S> value) throws Exception {
//				return value.left();
//			}
//		});
//		
//		DataStream<S> forwardStream = splitStream.select("FORWARD").map(new MapFunction<Either<R, S>, S>() {
//			@Override
//			public S map(Either<R, S> value) throws Exception {
//				return value.right();
//			}
//		});
		
	}

	public DataStream<F> closeWith(DataStream<F> feedbackStream) {

		Collection<StreamTransformation<?>> predecessors = feedbackStream.getTransformation().getTransitivePredecessors();

		if (!predecessors.contains(this.coFeedbackTransformation)) {
			throw new UnsupportedOperationException(
				"Cannot close an iteration with a feedback DataStream that does not originate from said iteration.");
		}

		coFeedbackTransformation.addFeedbackEdge(feedbackStream.getTransformation());

		return feedbackStream;
	}
}
