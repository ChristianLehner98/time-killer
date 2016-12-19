package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.windowing.TerminationFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.types.Either;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Internal
public class TwoWindowTerminateOperator<K, IN1, IN2, ACC1, ACC2, R, S, W1 extends Window, W2 extends Window>
	extends AbstractStreamOperator<Either<R,S>>
	implements TwoInputStreamOperator<IN1, IN2, Either<R,S>>, Serializable {

	WindowOperator<K, IN1, ACC1, Either<R,S>, W1> winOp1;
	WindowOperator<K, IN2, ACC2, Either<R,S>, W2> winOp2;
	TerminationFunction terminationFunction;
	Set<List<Long>> activeIterations = new HashSet<>();
	//StreamIterationTermination terminationStrategy;

	StreamTask<?, ?> containingTask;

	TimestampedCollector<Either<R,S>> collector;

	public TwoWindowTerminateOperator(WindowOperator winOp1, WindowOperator winOp2, TerminationFunction terminationFunction) {
		this.winOp1 = winOp1;
		this.winOp2 = winOp2;
		this.terminationFunction = terminationFunction;
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<Either<R,S>>> output) {
		super.setup(containingTask, config, output);

		// setup() both with own output
		StreamConfig config1 = new StreamConfig(config.getConfiguration().clone());
		config1.setOperatorName("WinOp1");
		StreamConfig config2 = new StreamConfig(config.getConfiguration().clone());
		config2.setOperatorName("WinOp2");
		winOp1.setup(containingTask, config1, output);
		winOp2.setup(containingTask, config2, output);

		this.containingTask = containingTask;
	}

	@Override
	public final void open() throws Exception {
		collector = new TimestampedCollector<>(output);

		OperatorStateHandles stateHandles = null;
		winOp1.getOperatorConfig().setStateKeySerializer(config.getStateKeySerializer(containingTask.getUserCodeClassLoader()));
		winOp2.getOperatorConfig().setStateKeySerializer(config.getStateKeySerializer(containingTask.getUserCodeClassLoader()));

		winOp1.initializeState(stateHandles);
		winOp2.initializeState(stateHandles);

		super.open();
		winOp1.open("window-timers1");
		winOp2.open("window-timers2");
	}

	@Override
	public final void close() throws Exception {
		super.close();
		winOp1.close();
		winOp2.close();
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();
		winOp1.dispose();
		winOp2.dispose();
	}

	public void processElement1(StreamRecord<IN1> element) throws Exception {
		//terminationStrategy.observeRecord(element);
		//if(terminationStrategy.terminate(element.getContext())) {
		//	collector.setAbsoluteTimestamp(element.getContext(), element.getTimestamp());
		//} else {
			activeIterations.add(element.getContext());
			winOp1.processElement(element);
		//}
	}
	public void processElement2(StreamRecord<IN2> element) throws Exception {
		//terminationStrategy.observeRecord(element);
		//if(terminationStrategy.terminate(element.getContext())) {
		//	collector.setAbsoluteTimestamp(element.getContext(), element.getTimestamp());
		//} else {
		if(activeIterations.contains(element.getContext())) {
			winOp2.processElement(element);
		}
		//}
	}

	public void processWatermark1(Watermark mark) throws Exception {
		//terminationStrategy.observeWatermark(mark);
		//if(terminationStrategy.terminate(mark.getContext())) {
		//	winOp1.processWatermark(new Watermark(mark.getContext(), Long.MAX_VALUE));
		//	if(mark.getContext().get(mark.getContext().size()-1) != Long.MAX_VALUE ) {
		//		terminationFunction.onTermination(mark.getContext(), collector);
		//	}
		//} else {
			winOp1.processWatermark(mark);
		//}
	}
	public void processWatermark2(Watermark mark) throws Exception {
		//terminationStrategy.observeWatermark(mark);
		//if(terminationStrategy.terminate(mark.getContext())) {
		if(mark.iterationDone()) {
			activeIterations.remove(mark.getContext());
			if(mark.getContext().get(mark.getContext().size()-1) != Long.MAX_VALUE ) {
				terminationFunction.onTermination(mark.getContext(), collector);
			}
			winOp2.processWatermark(new Watermark(mark.getContext(), Long.MAX_VALUE, false, mark.iterationOnly()));
		} else {
			winOp2.processWatermark(mark);
		}
	}

	public void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception {}
	public void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception {}
}
