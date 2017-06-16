package org.apache.flink.streaming.runtime.operators.windowing;

import akka.actor.ActorRef;
import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.progress.Notifyable;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.progress.messages.ProgressMetricsReport;
import org.apache.flink.streaming.api.functions.windowing.LoopContext;
import org.apache.flink.streaming.api.functions.windowing.WindowLoopFunction;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

@Internal
public class TwoWindowTerminateOperator<K, IN1, IN2, ACC1, ACC2, R, S, W1 extends Window, W2 extends Window>
	extends AbstractStreamOperator<Either<R,S>>
	implements TwoInputStreamOperator<IN1, IN2, Either<R,S>>, Serializable {

	public final static Logger logger = LoggerFactory.getLogger(TwoWindowTerminateOperator.class);
	private final KeySelector<IN1, K> entryKeying;
	WindowOperator<K, IN2, ACC2, Either<R,S>, W2> winOp2;
	WindowLoopFunction loopFunction;
	private StreamIterationTermination terminationStrategy;
	
	Set<List<Long>> activeIterations = new HashSet<>();

	StreamTask<?, ?> containingTask;

	TimestampedCollector<Either<R,S>> collector;

	//TODO implement this properly in a ProcessFunction and managed state
	Map<List<Long>, Map<K, List<IN1>>> entryBuffer;

	// MY METRICS
	private Map<List<Long>, Long> lastWinStartPerContext = new HashMap<>();
	private Map<List<Long>, Long> lastLocalEndPerContext = new HashMap<>();

	public TwoWindowTerminateOperator(KeySelector<IN1,K> entryKeySelector, WindowOperator winOp2, WindowLoopFunction loopFunction, StreamIterationTermination terminationStrategy) {
		this.entryKeying = entryKeySelector;
		this.winOp2 = winOp2;
		this.loopFunction = loopFunction;
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
		StreamConfig config2 = new StreamConfig(config.getConfiguration().clone());
		config2.setOperatorName("WinOp2");
		winOp2.setup(containingTask, config2, output);
		this.containingTask = containingTask;
	}

	@Override
	public final void open() throws Exception {
		collector = new TimestampedCollector<>(output);

		OperatorStateHandles stateHandles = null;
		winOp2.getOperatorConfig().setStateKeySerializer(config.getStateKeySerializer(containingTask.getUserCodeClassLoader()));
		winOp2.initializeState(stateHandles);

		super.open();
		winOp2.open("window-timers");
	}

	@Override
	public final void close() throws Exception {
		super.close();
		winOp2.close();
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();
		winOp2.dispose();
	}

	public void processElement1(StreamRecord<IN1> element) throws Exception {
		logger.info(getRuntimeContext().getIndexOfThisSubtask() +":: TWOWIN Received e from IN - "+ element);
		activeIterations.add(element.getContext());

		terminationStrategy.observeRecord(element);

		if(!entryBuffer.containsKey(element.getContext())){
			entryBuffer.put(element.getContext(), new HashMap<K, List<IN1>>());
		}

		Map<K, List<IN1>> tmp = entryBuffer.get(element.getContext());
		K key = entryKeying.getKey(element.getValue());
		if(!tmp.containsKey(key)){
			tmp.put(key, new ArrayList<IN1>());
		}
		tmp.get(key).add(element.getValue());

		final List<Long> context = element.getContext();
		notifyOnce(element.getFullTimestamp(), new Notifyable() {
			@Override
			public void receiveProgressNotification(List<Long> timestamp, boolean done) throws Exception {
				long iterationId = timestamp.remove(timestamp.size()-1);
				long lastWinStart = System.currentTimeMillis();
				System.out.println(new Watermark(timestamp, iterationId) + " @ " + getRuntimeContext().getIndexOfThisSubtask());
				if(entryBuffer.containsKey(context)){
					for(Map.Entry<K, List<IN1>> entry : entryBuffer.get(context).entrySet()){
						collector.setAbsoluteTimestamp(context,0);
						loopFunction.entry(new LoopContext(context, 0, entry.getKey()), entry.getValue(), collector);
					}
				}
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
					loopFunction.onTermination(nextTs, iterationId, collector);
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
		logger.info(getRuntimeContext().getIndexOfThisSubtask() +":: TWOWIN Received e from FEEDBACK - "+ element);
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

	public void sendMetrics(long windowEnd, List<Long> context) {
		if (getContainingTask().getEnvironment().getExecutionConfig().isExperimentMetricsEnabled()) {
			getContainingTask().getEnvironment().getJobManagerRef().tell(
				new ProgressMetricsReport(getContainingTask().getEnvironment().getJobID(),
					getOperatorConfig().getVertexID(),
					getRuntimeContext().getIndexOfThisSubtask(),
					context, new Long(0), lastWinStartPerContext.get(context), lastLocalEndPerContext.get(context), windowEnd
				), ActorRef.noSender());
		}
	}
}
