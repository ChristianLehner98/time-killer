package org.apache.flink.streaming.api.datastream;


import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.TerminationFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.CoFeedbackTransformation;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Collection;

@Public
public class ConnectedWindowedIterativeStreams<IN,IN_W extends Window,F,KEY> {

	private StreamExecutionEnvironment environment;
	private WindowedStream<IN, KEY, IN_W> windowedStream1;
	private WindowedStream<F, KEY, TimeWindow> windowedStream2;

	// TODO: Was damit machen???
	private CoFeedbackTransformation<F> coFeedbackTransformation;

	public ConnectedWindowedIterativeStreams(WindowedStream<IN,KEY,IN_W> input,
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

	public <R, S> Tuple2<DataStream<R>, DataStream<S>> reduce(ReduceFunction reduce1, ReduceFunction reduce2, TerminationFunction termination) {
		// TODO: create a Transformation that produces two (differently typed) outputs in StreamGraphGenerator
		// somehow have one transformation that includes a TwoWindowTerminateOperator and can be used in two streams
		DataStream<R> output = new DataStream<>(environment, /* ?? */);
		DataStream<S> feedback = new DataStream(environment, /* ?? */);
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
