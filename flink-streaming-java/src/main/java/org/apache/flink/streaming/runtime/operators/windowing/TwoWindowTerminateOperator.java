package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.TerminationFunction;
import org.apache.flink.api.common.state.AppendingState;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.StateInitializationContext;
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

import java.io.Serializable;
import java.util.Collection;

@Internal
public class TwoWindowTerminateOperator<K1, K2, IN1, IN2, ACC1, ACC2, OUT, W1 extends Window, W2 extends Window>
	extends AbstractStreamOperator<OUT>
	implements TwoInputStreamOperator<IN1, IN2, OUT> {

	WindowOperator<K1, IN1, ACC1, OUT, W1> winOp1;
	WindowOperator<K2, IN2, ACC2, OUT, W2> winOp2;

	TerminationFunction terminationFunction;

	private StreamInputProgressHandler progressHandler;

	public TwoWindowTerminateOperator(WindowAssigner<? super IN1, W1> windowAssigner1,
									  WindowAssigner<? super IN1, W1> windowAssigner2,
									  TypeSerializer<W1> windowSerializer1,
									  TypeSerializer<W2> windowSerializer2,
									  KeySelector<IN1, K1> keySelector1,
									  KeySelector<IN2, K2> keySelector2,
									  TypeSerializer<K1> keySerializer1,
									  TypeSerializer<K2> keySerializer2,
									  StateDescriptor<? extends AppendingState<IN1, ACC1>, ?> windowStateDescriptor1,
									  StateDescriptor<? extends AppendingState<IN2, ACC2>, ?> windowStateDescriptor2,
									  InternalWindowFunction<ACC1, OUT, K1, W1> windowFunction1,
									  InternalWindowFunction<ACC2, OUT, K2, W2> windowFunction2,
									  Trigger<? super IN1, ? super W1> trigger1,
									  Trigger<? super IN2, ? super W2> trigger2,
									  long allowedLateness,
									  TerminationFunction terminationFunction) {
		// TODO combine windowFunction1 and 2 to one (Rich)Window(Terminate)Function and wrap them!

		winOp1 = new WindowOperator(windowAssigner1, windowSerializer1, keySelector1, keySerializer1,
			windowStateDescriptor1, windowFunction1, trigger1, allowedLateness);

		winOp2 = new WindowOperator(windowAssigner2, windowSerializer2, keySelector2, keySerializer2,
			windowStateDescriptor2, windowFunction2, trigger2, allowedLateness);

		this.terminationFunction = terminationFunction;

		progressHandler = new StreamInputProgressHandler(2);
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
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
		winOp1.processElement(element);
	}
	public void processElement2(StreamRecord<IN2> element) throws Exception {
		winOp2.processElement(element);
	}

	public void processWatermark1(Watermark mark) throws Exception {
		//watermarks need to be treated together because we're unioning here!
		Watermark next = progressHandler.getNextWatermark(mark, 0);
		if(next != null) {
			winOp1.processWatermark(mark);
		}
	}
	public void processWatermark2(Watermark mark) throws Exception {
		//watermarks need to be treated together because we're unioning here!
		Watermark next = progressHandler.getNextWatermark(mark, 1);
		if(next != null) {
			winOp2.processWatermark(mark);
		}
	}

	public void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception {}
	public void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception {}
}
