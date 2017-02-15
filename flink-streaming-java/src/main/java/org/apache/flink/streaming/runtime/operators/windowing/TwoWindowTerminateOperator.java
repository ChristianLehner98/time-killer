package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.progress.Notifyable;
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
import org.apache.flink.streaming.runtime.tasks.progress.StreamIterationTermination;
import org.apache.flink.types.Either;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Internal
public class TwoWindowTerminateOperator<K, IN1, IN2, ACC1, ACC2, R, S, W1 extends Window, W2 extends Window>
	extends AbstractStreamOperator<Either<R,S>>
	implements TwoInputStreamOperator<IN1, IN2, Either<R,S>>, Serializable {

	private WindowOperator<K, IN1, ACC1, Either<R,S>, W1> winOp1;
	private WindowOperator<K, IN2, ACC2, Either<R,S>, W2> winOp2;
	private TerminationFunction terminationFunction;
	private StreamIterationTermination terminationStrategy;

	private Set<List<Long>> activeIterations = new HashSet<>();

	private StreamTask<?, ?> containingTask;

	private TimestampedCollector<Either<R,S>> collector;

	public TwoWindowTerminateOperator(WindowOperator winOp1, WindowOperator winOp2, TerminationFunction terminationFunction, StreamIterationTermination terminationStrategy) {
		this.winOp1 = winOp1;
		this.winOp2 = winOp2;
		this.terminationFunction = terminationFunction;
		this.terminationStrategy = terminationStrategy;
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, final Output<StreamRecord<Either<R,S>>> output) {
		Output<StreamRecord<Either<R,S>>> dummyOutput = new Output<StreamRecord<Either<R,S>>>() {
			public void collect(StreamRecord<Either<R,S>> record) {
				collectInternalProgress(operatorId, record.getFullTimestamp(), -1);
				collectProgress(output.getTargetOperatorId(), record.getFullTimestamp(), 1);
				output.collect(record);
			}
			public void emitWatermark(Watermark mark){}
			public void emitLatencyMarker(LatencyMarker latencyMarker){}
			public Integer getTargetOperatorId() {return output.getTargetOperatorId();}
			public void close() {}
		};
		super.setup(containingTask, config, dummyOutput);

		// setup() both with own output
		StreamConfig config1 = new StreamConfig(config.getConfiguration().clone());
		config1.setOperatorName("WinOp1");
		StreamConfig config2 = new StreamConfig(config.getConfiguration().clone());
		config2.setOperatorName("WinOp2");
		winOp1.setup(containingTask, config1, dummyOutput);
		winOp2.setup(containingTask, config2, dummyOutput);

		winOp1.setProgressAggregator(progressAggregator);
		winOp2.setProgressAggregator(progressAggregator);

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
		activeIterations.add(element.getContext());

		notifyOnce(element.getFullTimestamp(), new Notifyable() {
			@Override
			public void receiveProgressNotification(List<Long> timestamp, boolean done) throws Exception {
				long iterationId = timestamp.remove(timestamp.size()-1);
				winOp1.processWatermark(new Watermark(timestamp, iterationId));
			}
		}, terminationStrategy.terminate(element.getContext()));

		winOp1.processElement(element);
	}

	public void processElement2(StreamRecord<IN2> element) throws Exception {
		notifyOnce(element.getFullTimestamp(), new Notifyable() {
			@Override
			public void receiveProgressNotification(List<Long> timestamp, boolean done) throws Exception {
				long iterationId = timestamp.remove(timestamp.size()-1);
				Watermark watermark = new Watermark(timestamp, iterationId);
				terminationStrategy.observeWatermark(watermark);
				winOp2.processWatermark(watermark);
				if(done) {
					activeIterations.remove(timestamp);
					terminationFunction.onTermination(timestamp, collector);
				}
			}
		}, terminationStrategy.terminate(element.getContext()));

		if(activeIterations.contains(element.getContext())) {
			terminationStrategy.observeRecord(element);
			winOp2.processElement(element);
		}
	}

	public void processWatermark1(Watermark mark) throws Exception {}
	public void processWatermark2(Watermark mark) throws Exception {}

	public void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception {}
	public void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception {}

	@Override
	public boolean wantsProgressNotifications() {
		return true;
	}
}
