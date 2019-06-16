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

/**
 * A demonstration of the problems with the persistent state and why it sems rather useless for general apllications
 */
public class SaveAndReadPersistentStateExample {

	private static final List<Tuple3<Long, Long, Long>> sampleStream = Lists.newArrayList(

		// key - value - timestamp

		//start
		new Tuple3<>(1L, 1L, 1000L),
		new Tuple3<>(2L, 1L, 1000L),
		new Tuple3<>(3L, 1L, 1000L),

		//sums will be loaded correctly, because last input key is the same as in the previous window(3)
		new Tuple3<>(1L, 2L, 2000L),
		new Tuple3<>(2L, 2L, 2000L),
		new Tuple3<>(3L, 2L, 2000L),

		//sums will be loaded correctly, because last input key is the same as in the previous window(3)
		new Tuple3<>(5L, 3L, 3000L),
		new Tuple3<>(4L, 3L, 3000L),
		new Tuple3<>(3L, 3L, 3000L),

		//sums will not be loaded correctly, because last input key(1) is different from the one in all prevous windows
		new Tuple3<>(3L, 4L, 4000L),
		new Tuple3<>(2L, 4L, 4000L),
		new Tuple3<>(1L, 4L, 4000L),

		//(already incorrect) sums from previous window will be loaded, because last input key is the same as in the previous window(1)
		new Tuple3<>(4L, 5L, 5000L),
		new Tuple3<>(2L, 5L, 5000L),
		new Tuple3<>(1L, 5L, 5000L),

		//sums will not be loaded correctly, because last input key(2) is different from the one in all prevous windows
		new Tuple3<>(4L, 6L, 6000L),
		new Tuple3<>(1L, 6L, 6000L),
		new Tuple3<>(2L, 6L, 6000L),

		//sums from the first 3 windows (timestamps 1000-3000) will be loaded again, because last input key is the same as the one there(3)
		new Tuple3<>(1L, 7L, 7000L),
		new Tuple3<>(2L, 7L, 7000L),
		new Tuple3<>(3L, 7L, 7000L),

		//sums from the windows with timestamps 4000-5000 will be loaded again, because last input key is the same as the one there(1)
		new Tuple3<>(3L, 8L, 8000L),
		new Tuple3<>(2L, 8L, 8000L),
		new Tuple3<>(1L, 8L, 8000L),

		//sums from the windows with timestamp 6000 will be loaded again, because last input key is the same as the one there(2)
		new Tuple3<>(8L, 9L, 9000L),
		new Tuple3<>(9L, 9L, 9000L),
		new Tuple3<>(2L, 9L, 9000L),

		//sums will not be loaded correctly, because last input key(6) is different from the one in all prevous windows
		new Tuple3<>(2L, 10L, 10000L),
		new Tuple3<>(4L, 10L, 10000L),
		new Tuple3<>(6L, 10L, 10000L),

		//due to the watermarking in the source this will not enter the iteration at all, ignore this here
		new Tuple3<>(0L, 0L, 11000L)

	);
	private static Logger LOG = LoggerFactory.getLogger(SaveAndReadPersistentStateExample.class);
	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

	public SaveAndReadPersistentStateExample(int sleepTimePerElement, StateMode stateMode, OverwriteMode overwriteMode,
		StateAccessMode stateAccessMode) throws Exception {
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		//parallelism 1 only for testing purposes
		env.setParallelism(1);

		//create the windowed input stream
		DataStream<Tuple2<Long, Long>> input = env.addSource(new PersistentStateSampleSrc(sleepTimePerElement));
		WindowedStream<Tuple2<Long, Long>, Long, TimeWindow> winInput = input
			.keyBy(new KeySelector<Tuple2<Long, Long>, Long>() {
				@Override
				public Long getKey(Tuple2<Long, Long> value) throws Exception {
					return value.f0;
				}
			}).timeWindow(Time.milliseconds(1000));

		//execute the iteration
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


	public enum StateMode {
		/**
		 * a MapState is used for persistent state
		 */
		MAPSTATE,

		/**
		 * persistent state is just simulated with a map, just for testing purposes. Works fine here since state is quite small.
		 */
		MAP;

		public static StateMode parse(String mode) {
			if (mode.equalsIgnoreCase("Map")) {
				return MAP;
			} else if (mode.equalsIgnoreCase("MapState")) {
				return MAPSTATE;
			} else {
				return null;
			}
		}
	}

	public enum OverwriteMode {

		/**
		 * The state is overwritten directly in the entry function for every key
		 */
		IN_ENTRY,

		/**
		 * the state is overwitten at the end of each window in the onTermination function
		 */
		IN_ON_TERMINATION;

		public static OverwriteMode parse(String mode) {
			if (mode.equalsIgnoreCase("entry")) {
				return IN_ENTRY;
			} else if (mode.equalsIgnoreCase("onTermination")) {
				return IN_ON_TERMINATION;
			} else {
				return null;
			}
		}
	}

	public enum StateAccessMode {

		/**
		 * state = ctx.getRuntimeContext().getMapState() is only executed when the state is null
		 */
		ONCE,

		/**
		 * state = ctx.getRuntimeContext().getMapState() is executed every time before the state is accessed
		 */
		PER_ACCESS;

		public static StateAccessMode parse(String mode) {
			if (mode.equalsIgnoreCase("once")) {
				return ONCE;
			} else if (mode.equalsIgnoreCase("perAccess")) {
				return PER_ACCESS;
			} else {
				return null;
			}
		}
	}

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
				System.err.println("sleep time per element: " + sleepTimePerElement + " ms");
				LOG.debug("sleep time per element: {} ms", sleepTimePerElement);
				ctx.collectWithTimestamp(new Tuple2<>(next.f0, next.f1), next.f2);

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

			System.err.println(
				(ctx.getRuntimeContext().getIndexOfThisSubtask() + 1) + "> " + "[state]:: " + "ctx:" + ctx
					+ " NEW ITERATION - EXISTING STATE: " + getState());
			LOG.debug("{}> [state]:: ctx:{} NEW ITERATION - EXISTING STATE: {}",
				ctx.getRuntimeContext().getIndexOfThisSubtask() + 1, ctx, getState());


			Map<Long, Long> sums = sumsPerContext.get(ctx.getContext());
			if (sums == null) {
				sums = new HashMap<>();
				sumsPerContext.put(ctx.getContext(), sums);
			}

			//load old sum
			long sum = 0;
			switch (stateMode) {
				case MAPSTATE:
					if (persistentSumsState.contains(ctx.getKey())) {
						sum = persistentSumsState.get(ctx.getKey());
					}
					break;
				case MAP:
					sum = persistentSumsMap.getOrDefault(ctx.getKey(), 0L);
					break;
			}

			//calculate new sum
			for (Tuple2<Long, Long> entry : entries) {
				sum += entry.f1;
			}

			//save new some for this window
			sums.put(ctx.getKey(), sum);

			System.err.println((ctx.getRuntimeContext().getIndexOfThisSubtask() + 1) + "> "
				+ "ENTRY (" + ctx.getKey() + "):: " + "new entires: " + entries + ", new sum: " + sum);
			LOG.debug("{}> ENTRY ({}):: new entries: {}, new sum: {}",
				ctx.getRuntimeContext().getIndexOfThisSubtask() + 1, ctx.getKey(), entries, sum);

			//if persistent state should be overwritten in the entry function, overwrtie persistent state
			if (overwriteMode.equals(OverwriteMode.IN_ENTRY)) {
				System.err.println(
					(ctx.getRuntimeContext().getIndexOfThisSubtask() + 1) + "> " + "[state]:: " + "ctx:" + ctx
						+ " UPDATING Persistent State");
				LOG.debug("{}> [state]:: ctx:{} UPDATING Persistent State",
					ctx.getRuntimeContext().getIndexOfThisSubtask() + 1, ctx);
				switch (stateMode) {
					case MAPSTATE:
						persistentSumsState.put(ctx.getKey(), sum);
						break;
					case MAP:
						persistentSumsMap.put(ctx.getKey(), sum);
						break;
				}
				System.err
					.println((ctx.getRuntimeContext().getIndexOfThisSubtask() + 1) + "> " + "[state]:: " + "ctx:"
						+ ctx + " Current State is:" + getState());
				LOG.debug("{}> [state]:: ctx:{} Current State is {}",
					ctx.getRuntimeContext().getIndexOfThisSubtask() + 1, ctx, getState());
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

			//get all the sums after this window
			Map<Long, Long> sums = sumsPerContext.get(ctx.getContext());
			System.err.println(
				(ctx.getRuntimeContext().getIndexOfThisSubtask() + 1) + "> " + "ON TERMINATION:: ctx: " + ctx + " :: "
					+ sums);
			LOG.debug("{}> ON TERMINATION:: ctx: {} :: {}", ctx.getRuntimeContext().getIndexOfThisSubtask() + 1, ctx,
				sums);

			//if persistent state should be overwritten in the onTermination function and there were entries
			//in this window, overwrite persistent state
			if (overwriteMode.equals(OverwriteMode.IN_ON_TERMINATION) && sumsPerContext.containsKey(ctx.getContext())) {

				//TEST - BACK UP Snapshot to persistent state
				checkAndInitState(ctx);

				System.err.println(
					(ctx.getRuntimeContext().getIndexOfThisSubtask() + 1) + "> " + "[state]:: " + "ctx:" + ctx
						+ " UPDATING Persistent State");
				LOG.debug("{}> [state]:: ctx:{} UPDATING Persistent State",
					ctx.getRuntimeContext().getIndexOfThisSubtask() + 1, ctx);

				switch (stateMode) {
					case MAPSTATE:
						persistentSumsState.putAll(sumsPerContext.get(ctx.getContext()));
						break;
					case MAP:
						persistentSumsMap.putAll(sumsPerContext.get(ctx.getContext()));
						break;
				}

				System.err
					.println((ctx.getRuntimeContext().getIndexOfThisSubtask() + 1) + "> " + "[state]:: " + "ctx:"
						+ ctx + " Current State is:" + getState());
				LOG.debug("{}> [state]:: ctx:{} Current State is {}",
					ctx.getRuntimeContext().getIndexOfThisSubtask() + 1, ctx, getState());

			}

		}

		private void checkAndInitState(LoopContext<Long> ctx) throws Exception {
			System.err
				.println((ctx.getRuntimeContext().getIndexOfThisSubtask() + 1) + "> " + "check and init state called");
			LOG.debug("{}> check and init state called", ctx.getRuntimeContext().getIndexOfThisSubtask() + 1);

			//if desired or necessary get the state from the runtime context
			if (stateAccessMode.equals(StateAccessMode.PER_ACCESS) || checkStateIsNull()) {
				System.err.println((ctx.getRuntimeContext().getIndexOfThisSubtask() + 1) + "> "
					+ "INITIALIZING/LOADING persistent state");
				LOG.debug("{}> INITIALIZING/LOADING persistent state",
					ctx.getRuntimeContext().getIndexOfThisSubtask() + 1);

				switch (stateMode) {
					case MAPSTATE:
						persistentSumsState = ctx.getRuntimeContext().getMapState(
							new MapStateDescriptor<>("sums", LongSerializer.INSTANCE, LongSerializer.INSTANCE));
						System.err.println((ctx.getRuntimeContext().getIndexOfThisSubtask() + 1)
							+ "> LOADED PERSISTENT STATE :: ctx:" + ctx + ", MapState:" + persistentSumsState
							+ ", Current State is "
							+ getState());
						LOG.debug(
							"{}> LOADED PERSISTENT STATE :: ctx:{}, StreamingRuntimeContext:{}, MapState:{}, Current State is {}",
							ctx.getRuntimeContext().getIndexOfThisSubtask() + 1, ctx, ctx.getRuntimeContext(),
							persistentSumsState, getState());
						break;
					case MAP:
						persistentSumsMap = persistentSumsMap == null ? new HashMap<>() : persistentSumsMap;
						System.err.println((ctx.getRuntimeContext().getIndexOfThisSubtask() + 1) + "> "
							+ "LOADED PERSISTENT STATE [state]:: " + "ctx:" + ctx + " Current State is:"
							+ getState());
						LOG.debug(
							"{}> LOADED PERSISTENT STATE [state]:: ctx:{} Current State is {}",
							ctx.getRuntimeContext().getIndexOfThisSubtask() + 1, ctx, getState());
				}

			}

		}

		private boolean checkStateIsNull() throws Exception {
			switch (stateMode) {
				case MAPSTATE:
					return persistentSumsState == null;
				case MAP:
					return persistentSumsMap == null;
				default:
					return true;
			}
		}

		private String getState() throws Exception {

			if (checkStateIsNull()) {
				return "null";
			}
			switch (stateMode) {
				case MAPSTATE:
					return persistentSumsState.entries().toString();
				case MAP:
					return persistentSumsMap.entrySet().toString();
				default:
					return "";
			}
		}
	}
}
