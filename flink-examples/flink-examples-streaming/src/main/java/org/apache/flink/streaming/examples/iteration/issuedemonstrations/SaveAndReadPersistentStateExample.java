package org.apache.flink.streaming.examples.iteration.issuedemonstrations;


import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.FeedbackBuilder;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.LoopContext;
import org.apache.flink.streaming.api.functions.windowing.WindowLoopFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.tasks.progress.FixpointIterationTermination;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SaveAndReadPersistentStateExample {

	private static Logger LOG = LoggerFactory.getLogger(SaveAndReadPersistentStateExample.class);
	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public SaveAndReadPersistentStateExample(int sleepTimePerElement, StateMode stateMode, OverwriteMode overwriteMode,
		StateAccessMode stateAccessMode) throws Exception {
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		//parallelism 1 only for testing purposes
		env.setParallelism(1);

		DataStream<Tuple2<Long, Long>> input = env.addSource(new PersistentStateSampleSrc(sleepTimePerElement));
		WindowedStream<Tuple2<Long, Long>, Long, TimeWindow> winInput = input
			.keyBy(new KeySelector<Tuple2<Long, Long>, Long>() {
				@Override
				public Long getKey(Tuple2<Long, Long> value) throws Exception {
					return value.f0;
				}
			}).timeWindow(Time.milliseconds(1000));

		DataStream<Tuple2<Long, Long>> results = winInput.iterateSync(
			new PersistentStateWindowLoopFunction(stateMode, overwriteMode, stateAccessMode),
			new FixpointIterationTermination(),
			new FeedbackBuilder<Tuple2<Long, Long>, Long>() {

				@Override
				public KeyedStream<Tuple2<Long, Long>, Long> feedback(
					DataStream<Tuple2<Long, Long>> input) {

					return input.keyBy(new KeySelector<Tuple2<Long, Long>, Long>() {
						@Override
						public Long getKey(Tuple2<Long, Long> value) throws Exception {
							return value.f0;
						}
					});
				}
			}, new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO));

		results.print();

	}

	public void run() throws Exception {
		System.err.println(env.getExecutionPlan());
		env.execute("Persistent State Issue Demonstration");
	}

	private static final List<Tuple3<Long, Long, Long>> sampleStream = Lists.newArrayList(

		// key - value - timestamp
		new Tuple3<>(1L, 1L, 1000L),
		new Tuple3<>(2L, 1L, 1000L),
		new Tuple3<>(3L, 1L, 1000L),
		new Tuple3<>(1L, 1L, 2000L),
		new Tuple3<>(2L, 2L, 2000L),
		new Tuple3<>(3L, 3L, 2000L),
		new Tuple3<>(1L, 1L, 3000L),
		new Tuple3<>(2L, 3L, 3000L),
		new Tuple3<>(3L, 5L, 3000L),
		new Tuple3<>(1L, 1L, 4000L),
		new Tuple3<>(2L, 4L, 4000L),
		new Tuple3<>(3L, 7L, 4000L),
		new Tuple3<>(4L, 0L, 5000L)

	);

	private static class PersistentStateSampleSrc extends RichSourceFunction<Tuple2<Long, Long>> {

		private final long sleepTimePerElement;

		public PersistentStateSampleSrc(int sleepTimePerElement) {
			this.sleepTimePerElement = sleepTimePerElement;
		}

		@Override
		public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {
			long curTime = -1;
			for (Tuple3<Long, Long, Long> next : sampleStream) {
				Thread.sleep(sleepTimePerElement);
				System.err.println("sleep time per element: "+ sleepTimePerElement + " ms");
				LOG.debug("sleep time per element: {} ms", sleepTimePerElement);
				ctx.collectWithTimestamp(new Tuple2<>(next.f0, next.f1), next.f2);
//				System.err.println("input :: key: " + next.f0 + ", value: " + next.f1);
//				LOG.debug("input :: key: {}, value: {}", next.f0, next.f1);

				if (curTime == -1) {
					curTime = next.f2;
				}
				if (curTime < next.f2) {
					curTime = next.f2;
					ctx.emitWatermark(new Watermark(curTime - 1));
				}

			}
		}

		@Override
		public void cancel() {
		}
	}

    private static class PersistentStateWindowLoopFunction implements
        WindowLoopFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>, Long, TimeWindow> {

        private StateMode stateMode;
		private OverwriteMode overwriteMode;
		private StateAccessMode stateAccessMode;
		private Map<List<Long>, Map<Long, Long>> sumsPerContext = new HashMap<>();
        private MapState<Long, Long> persistentSumsState = null;
        private Map<Long, Long> persistentSumsMap = null;

        public PersistentStateWindowLoopFunction(StateMode stateMode, OverwriteMode overwriteMode,
			StateAccessMode stateAccessMode) {
        	this.stateMode = stateMode;
        	this.overwriteMode = overwriteMode;
        	this.stateAccessMode = stateAccessMode;
		}


        @Override
        public void entry(LoopContext<Long> ctx, Iterable<Tuple2<Long, Long>> entries,
            Collector<Either<Tuple2<Long, Long>, Tuple2<Long, Long>>> out) throws Exception {


			checkAndInitState(ctx);

			System.err.println((ctx.getRuntimeContext().getIndexOfThisSubtask() + 1)+ "> " + "[state]:: "+"ctx:"+ctx+" NEW ITERATION - EXISTING STATE: " + getState(ctx.getKey()));
			LOG.debug("{}> [state]:: ctx:{} NEW ITERATION - EXISTING STATE: {}", ctx.getRuntimeContext().getIndexOfThisSubtask() + 1, ctx, getState(ctx.getKey()));

			Map<Long, Long> sums = sumsPerContext.get(ctx.getContext());
			if (sums == null) {
				sums = new HashMap<>();
				sumsPerContext.put(ctx.getContext(), sums);
			}

			long sum = 0;

			switch(stateMode) {
				case MAPSTATE:
					if(persistentSumsState.contains(ctx.getKey())) {
						sum = persistentSumsState.get(ctx.getKey());
					}
					break;
				case MAP:
					sum = persistentSumsMap.getOrDefault(ctx.getKey(), 0L);
					break;
			}

			for(Tuple2<Long, Long> entry: entries) {
				sum += entry.f1;
			}

			sums.put(ctx.getKey(), sum);

			System.err.println((ctx.getRuntimeContext().getIndexOfThisSubtask() + 1)+ "> "
				+ "ENTRY (" + ctx.getKey() + "):: " + "new entires: " + entries + ", new sum: " + sum);
			LOG.debug("{}> ENTRY ({}):: new entries: {}, new sum: {}", ctx.getRuntimeContext().getIndexOfThisSubtask() + 1, ctx.getKey(), entries, sum);

			if(overwriteMode.equals(OverwriteMode.IN_ENTRY)) {
				System.err.println(
					(ctx.getRuntimeContext().getIndexOfThisSubtask() + 1) + "> " + "[state]:: " + "ctx:" + ctx
						+ " UPDATING Persistent State");
				LOG.debug("{}> [state]:: ctx:{} UPDATING Persistent State",
					ctx.getRuntimeContext().getIndexOfThisSubtask() + 1, ctx);
				switch(stateMode) {
					case MAPSTATE:
						persistentSumsState.put(ctx.getKey(), sum);
						break;
					case MAP:
						persistentSumsMap.put(ctx.getKey(), sum);
						break;
				}
				System.err
					.println((ctx.getRuntimeContext().getIndexOfThisSubtask() + 1) + "> " + "[state]:: " + "ctx:"
						+ ctx + " Current State is:" + getState(ctx.getKey()));
				LOG.debug("{}> [state]:: ctx:{} Current State is {}",
					ctx.getRuntimeContext().getIndexOfThisSubtask() + 1, ctx, getState(ctx.getKey()));
			}
        }

        @Override
        public void step(LoopContext<Long> ctx, Iterable<Tuple2<Long, Long>> componentMessages,
            Collector<Either<Tuple2<Long, Long>, Tuple2<Long, Long>>> out) throws Exception {

        	//not needed

        }

        @Override
        public void onTermination(LoopContext<Long> ctx,
            Collector<Either<Tuple2<Long, Long>, Tuple2<Long, Long>>> out) throws Exception {

			Map<Long, Long> sums = sumsPerContext.get(ctx.getContext());
			System.err.println((ctx.getRuntimeContext().getIndexOfThisSubtask() + 1)+ "> " + "ON TERMINATION:: ctx: " + ctx + " :: " + sums);
			LOG.debug("{}> ON TERMINATION:: ctx: {} :: {}", ctx.getRuntimeContext().getIndexOfThisSubtask() + 1, ctx, sums);



			if(overwriteMode.equals(OverwriteMode.IN_ON_TERMINATION) && sumsPerContext.containsKey(ctx.getContext())) {

				//TEST - BACK UP Snapshot to persistent state
				checkAndInitState(ctx);

				System.err.println(
					(ctx.getRuntimeContext().getIndexOfThisSubtask() + 1) + "> " + "[state]:: " + "ctx:" + ctx
						+ " UPDATING Persistent State");
				LOG.debug("{}> [state]:: ctx:{} UPDATING Persistent State",
					ctx.getRuntimeContext().getIndexOfThisSubtask() + 1, ctx);

				switch(stateMode) {
					case MAPSTATE:
						persistentSumsState.putAll(sumsPerContext.get(ctx.getContext()));
						break;
					case MAP:
						persistentSumsMap.putAll(sumsPerContext.get(ctx.getContext()));
						break;
				}

				System.err
					.println((ctx.getRuntimeContext().getIndexOfThisSubtask() + 1) + "> " + "[state]:: " + "ctx:"
						+ ctx + " Current State is:" + getState(ctx.getKey()));
				LOG.debug("{}> [state]:: ctx:{} Current State is {}",
					ctx.getRuntimeContext().getIndexOfThisSubtask() + 1, ctx, getState(ctx.getKey()));

			}

        }

		private void checkAndInitState(LoopContext<Long> ctx) throws Exception {
			System.err.println((ctx.getRuntimeContext().getIndexOfThisSubtask() + 1)+ "> " + "check and init state called");
			LOG.debug("{}> check and init state called", ctx.getRuntimeContext().getIndexOfThisSubtask() + 1);

			if(stateAccessMode.equals(StateAccessMode.PER_ACCESS) || checkStateIsNull()){
				System.err.println((ctx.getRuntimeContext().getIndexOfThisSubtask() + 1)+ "> " + "INITIALIZING/LOADING persistent state");
				LOG.debug("{}> INITIALIZING/LOADING persistent state", ctx.getRuntimeContext().getIndexOfThisSubtask() + 1);

				switch(stateMode) {
					case MAPSTATE:
						persistentSumsState = ctx.getRuntimeContext().getMapState(new MapStateDescriptor<>("sums", LongSerializer.INSTANCE, LongSerializer.INSTANCE));
						break;
					case MAP:
						persistentSumsMap = persistentSumsMap == null ? new HashMap<>() : persistentSumsMap;
				}
				
				System.err.println((ctx.getRuntimeContext().getIndexOfThisSubtask() + 1) + "> "
				+ "LOADED PERSISTENT STATE [state]:: " + "ctx:" + ctx + " Current State is:"
				+ getState(ctx.getKey()));
			LOG.debug(
				"{}> LOADED PERSISTENT STATE [state]:: ctx:{} Current State is {}",
				ctx.getRuntimeContext().getIndexOfThisSubtask() + 1, ctx, getState(ctx.getKey()));

			}

		}

		private boolean checkStateIsNull() throws Exception{
        	switch(stateMode) {
				case MAPSTATE:
					return persistentSumsState == null;
				case MAP:
					return persistentSumsMap == null;
				default:
					return true;
			}
		}

		private String getState(Long key) throws Exception {

        	if(checkStateIsNull()) {
        		return "null";
			}
			switch(stateMode) {
				case MAPSTATE:
					return persistentSumsState.entries().toString();
				case MAP:
					return persistentSumsMap.entrySet().toString();
				default:
					return "";
			}
		}
	}

    public enum StateMode {
		MAPSTATE, MAP;

		public static StateMode parse(String mode) {
			if(mode.equalsIgnoreCase("Map")) return MAP;
			else if(mode.equalsIgnoreCase("MapState")) return MAPSTATE;
			else return null;
		}
	}

	public enum OverwriteMode {
		IN_ENTRY, IN_ON_TERMINATION;

		public static OverwriteMode parse(String mode) {
			if(mode.equalsIgnoreCase("entry")) return IN_ENTRY;
			else if(mode.equalsIgnoreCase("onTermination")) return IN_ON_TERMINATION;
			else return null;
		}
	}

	public enum StateAccessMode {
		ONCE, PER_ACCESS;

		public static StateAccessMode parse(String mode) {
			if (mode.equalsIgnoreCase("once")) return ONCE;
			else if (mode.equalsIgnoreCase("perAccess")) return PER_ACCESS;
			else return null;
		}
	}
}
