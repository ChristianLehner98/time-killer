package org.apache.flink.streaming.runtime.operators.windowing;

import akka.actor.ActorRef;
import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.progress.Notifyable;
import org.apache.flink.runtime.progress.messages.ProgressMetricsReport;
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
import org.apache.flink.streaming.runtime.tasks.SystemProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.progress.StreamIterationTermination;
import org.apache.flink.types.Either;

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedList;
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
				//collectProgress(output.getTargetOperatorId(), record.getFullTimestamp(), 1);
				output.collect(record);
			}
			public void emitWatermark(Watermark mark){}
			public void emitLatencyMarker(LatencyMarker latencyMarker){}
			public Integer getTargetOperatorId(StreamRecord record) {return output.getTargetOperatorId(record);}
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

		//winOp1.setProgressAggregator(progressAggregator);
		//winOp2.setProgressAggregator(progressAggregator);

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
		System.out.println("INSTANCE: " + getRuntimeContext().getIndexOfThisSubtask());
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
		terminationStrategy.observeRecord(element);

		// window start
		winOp1.processElement(element);

		final List<Long> context = element.getContext();
		notifyOnce(element.getFullTimestamp(), new Notifyable() {
			@Override
			public void receiveProgressNotification(List<Long> timestamp, boolean done) throws Exception {
				long iterationId = timestamp.remove(timestamp.size()-1);
				long lastWinStart = System.currentTimeMillis();
				System.out.println(new Watermark(timestamp, iterationId) + " @ " + getRuntimeContext().getIndexOfThisSubtask());
				winOp1.processWatermark(new Watermark(timestamp, iterationId));
				long lastLocalEnd = System.currentTimeMillis();
				List<Long> nextTimestamp = new LinkedList<>(timestamp);
				nextTimestamp.add(iterationId + 1);
				notifyNext(nextTimestamp, context, lastWinStart,lastLocalEnd);
			}
		}, terminationStrategy.terminate(context));
	}

	private void notifyNext(final List<Long> nextTimestamp, final List<Long> context, final long lastWinStart, final long lastLocalEnd) {
		notifyOnce(nextTimestamp, new Notifyable() {
			@Override
			public void receiveProgressNotification(List<Long> nextTs, boolean done) throws Exception {
				long iterationId = nextTs.remove(nextTs.size() - 1);
				if (done) {
					activeIterations.remove(nextTs);
					terminationFunction.onTermination(nextTs, collector);
				} else {
					getContainingTask().getEnvironment().getJobManagerRef().tell(
						new ProgressMetricsReport(getContainingTask().getEnvironment().getJobID(),
							getOperatorConfig().getVertexID(),
							getRuntimeContext().getIndexOfThisSubtask(),
							context, iterationId, lastWinStart,lastLocalEnd,System.currentTimeMillis()
							), ActorRef.noSender());
					Watermark watermark = new Watermark(nextTs, iterationId);
					System.out.println(watermark + " @ " + getRuntimeContext().getIndexOfThisSubtask());
					terminationStrategy.observeWatermark(watermark);
					long nextWinStart = System.currentTimeMillis();
					winOp2.processWatermark(watermark);
					long nextLocalEnd = System.currentTimeMillis();
					List<Long> nextNextTimestamp = new LinkedList<>(nextTs);
					nextNextTimestamp.add(iterationId + 1);
					notifyNext(nextNextTimestamp, nextTs, nextWinStart, nextLocalEnd);
				}
			}
		}, terminationStrategy.terminate(context));
		winOp2.sendProgress();
	}

	public void processElement2(StreamRecord<IN2> element) throws Exception {
		if (activeIterations.contains(element.getContext())) {
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
