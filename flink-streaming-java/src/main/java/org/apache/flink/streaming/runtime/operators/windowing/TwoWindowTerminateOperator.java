package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.AppendingState;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.windowing.TerminationFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.io.StreamInputProgressHandler;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.types.Either;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Internal
public class TwoWindowTerminateOperator<K, IN1, IN2, ACC1, ACC2, R, S, W1 extends Window, W2 extends Window>
	extends AbstractStreamOperator<Either<R,S>>
	implements TwoInputStreamOperator<IN1, IN2, Either<R,S>> {

	WindowOperator<K, IN1, ACC1, Either<R,S>, W1> winOp1;
	WindowOperator<K, IN2, ACC2, Either<R,S>, W2> winOp2;
	TerminationFunction terminationFunction;

	private StreamInputProgressHandler progressHandler;
	private Map<List<Long>,Long> iterationIndices = new HashMap<>();

	public TwoWindowTerminateOperator(WindowOperator winOp1, WindowOperator winOp2, TerminationFunction terminationFunction) {
		this.winOp1 = winOp1;
		this.winOp2 = winOp2;
		this.terminationFunction = terminationFunction;
		progressHandler = new StreamInputProgressHandler(2);
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<Either<R,S>>> output) {
		// setup() both with own output
		winOp1.setup(containingTask, config, output);
		winOp2.setup(containingTask, config, output);

		OperatorStateHandles stateHandles = null;
		try {
			winOp1.initializeState(stateHandles);
			winOp2.initializeState(stateHandles);
		} catch(Exception e) {}

		super.setup(containingTask, config, output);
	}

	@Override
	public final void open() throws Exception {
		winOp1.open();
		winOp2.open();
		super.open();
	}

	@Override
	public final void close() throws Exception {
		winOp1.close();
		winOp2.close();
		super.close();
	}

	@Override
	public void dispose() throws Exception {
		winOp1.dispose();
		winOp2.dispose();
		super.dispose();
	}

	public void processElement1(StreamRecord<IN1> element) throws Exception {
		// TODO include iteration index somehow
		winOp1.processElement(element);
	}
	public void processElement2(StreamRecord<IN2> element) throws Exception {
		// TODO Only do this if the context still exists??
		// TODO include iteration index somehow
		winOp2.processElement(element);
	}

	public void processWatermark1(Watermark mark) throws Exception {
		//watermarks need to be treated together because we're unioning here!
		Watermark next = progressHandler.getNextWatermark(mark, 0);
		if(next != null) {
			iterationIndices.put(mark.getContext(), mark.getTimestamp());
			winOp2.processWatermark(mark);

			if(terminationFunction.terminate()) {
				terminateIteration(mark);
			}
		}
	}
	public void processWatermark2(Watermark mark) throws Exception {
		//watermarks need to be treated together because we're unioning here!
		Watermark next = progressHandler.getNextWatermark(mark, 1);
		if(next != null) {
			if(mark.getTimestamp() == Long.MAX_VALUE || terminationFunction.terminate()) {
				terminateIteration(mark);
			} else {
				// TODO only to Feedback?
				winOp2.processWatermark(mark);
				iterationIndices.put(mark.getContext(), mark.getTimestamp());
			}
		}
	}

	private void terminateIteration(Watermark mark) {
		if(iterationIndices.get(mark.getContext()) != null) {
			terminationFunction.onTermination();
			iterationIndices.remove(mark.getContext());
			// TODO send watermark to output!
		}
	}

	public void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception {}
	public void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception {}
}
