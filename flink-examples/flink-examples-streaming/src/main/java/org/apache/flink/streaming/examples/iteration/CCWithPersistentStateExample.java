package org.apache.flink.streaming.examples.iteration;


import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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

/**
 * This example onl ydemostartes the use of persistent state
 *
 * The algorithm itself is quite irrelevant and not the point of this example.
 */
public class CCWithPersistentStateExample {


    private static final List<Tuple3<Long, Long, Long>> sampleStream = Lists.newArrayList(

        // vertex1 - vertex2 - timestamp
        new Tuple3<>(1L, 2L, 1L),
        new Tuple3<>(2L, 3L, 1L),
        new Tuple3<>(4L, 5L, 1L),
        new Tuple3<>(1L, 3L, 1L),
        new Tuple3<>(6L, 7L, 2L),
        new Tuple3<>(3L, 7L, 2L),
        new Tuple3<>(8L, 9L, 3L),
        new Tuple3<>(9L, 10L, 3L),
        new Tuple3<>(9L, 11L, 4L),
        new Tuple3<>(6L, 12L, 4L),
        new Tuple3<>(13L, 14L, 5L),
        new Tuple3<>(15L, 16L, 5L)

    );

    public static void main(String args[]) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //parallelism 1 only for testing purposes
        env.setParallelism(4);

        DataStream<Tuple2<Long, Long>> input = env.addSource(new CCSampleSrc()).flatMap(
            new FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
                @Override
                public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Long>> out)
                    throws Exception {
                    out.collect(element);
                    out.collect(new Tuple2<>(element.f1, element.f0));
                }
            });
        KeyedStream<Tuple2<Long, Long>, Long> keyedInput = input
            .keyBy(new KeySelector<Tuple2<Long, Long>, Long>() {
                @Override
                public Long getKey(Tuple2<Long, Long> value) throws Exception {
                    return value.f0;
                }
            });

//        keyedInput.print();

        WindowedStream<Tuple2<Long, Long>, Long, TimeWindow> windowedInput = keyedInput
            .timeWindow(Time.milliseconds(1));

        DataStream<Tuple2<Long, Long>> results = windowedInput.iterateSync(
            new CCWindowLoopFunction(), new FixpointIterationTermination(),
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

        System.err.println(env.getExecutionPlan());
        env.execute();
    }

    private static class CCSampleSrc extends RichSourceFunction<Tuple2<Long, Long>> {

        @Override
        public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {
            long curTimestamp = -1;
            for (Tuple3<Long, Long, Long> next : sampleStream) {
            	Thread.sleep(2500);

				if (curTimestamp == -1) {
					curTimestamp = next.f2;
				}
				if (curTimestamp < next.f2) {
					ctx.emitWatermark(new Watermark(curTimestamp));
					curTimestamp = next.f2;
				}

                ctx.collectWithTimestamp(new Tuple2<>(next.f0, next.f1), next.f2);


            }
			Thread.sleep(2500);
            ctx.emitWatermark(new Watermark(curTimestamp));
        }

        @Override
        public void cancel() {
        }
    }

    private static class CCWindowLoopFunction implements
		WindowLoopFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>, Long, TimeWindow> {

        private transient ListState<Long> neighborsState = null;
        private transient ValueState<Long> componentState = null;


        @Override
        public void entry(LoopContext<Long> loopContext, Iterable<Tuple2<Long, Long>> edges,
            Collector<Either<Tuple2<Long, Long>, Tuple2<Long, Long>>> out) throws Exception {

            checkAndInitState(loopContext);

            if(componentState.value() == null) {
            	componentState.update(loopContext.getKey());
			}

			System.err.println(loopContext.getRuntimeContext().getIndexOfThisSubtask()+ "> " + "ENTRY: " + loopContext.getKey()+ ", component: " + componentState.value());

            for (Tuple2<Long, Long> edge : edges) {
                neighborsState.add(edge.f1);
                //send own component to new neighbor
                out.collect(Either.Left(new Tuple2<>(edge.f1, componentState.value())));
                System.err.println(loopContext.getRuntimeContext().getIndexOfThisSubtask()+ "> " + "ENTRY message sent from " + edge.f0 + " to " + edge.f1 + ": " + componentState.value());
            }

        }

        @Override
        public void step(LoopContext<Long> loopContext, Iterable<Tuple2<Long, Long>> componentMessages,
            Collector<Either<Tuple2<Long, Long>, Tuple2<Long, Long>>> out) throws Exception {

			checkAndInitState(loopContext);

            long oldComponent;

            long newComponent = oldComponent = componentState.value();

			System.err.println(loopContext.getRuntimeContext().getIndexOfThisSubtask()+ "> " + "STEP: " + loopContext.getKey()+ ", old component: " + oldComponent);

            for (Tuple2<Long, Long> msg : componentMessages) {
				System.err.println(loopContext.getRuntimeContext().getIndexOfThisSubtask()+ "> " + "STEP message received at vertex " + msg.f0 + ": " + msg.f1);
                //update component
                if (msg.f1 < newComponent) {
                    newComponent = msg.f1;
                }
            }

			System.err.println(loopContext.getRuntimeContext().getIndexOfThisSubtask()+ "> " + "STEP: " + loopContext.getKey()+ ", new component: " + newComponent);

            if(newComponent != oldComponent) {
                //save new component locally
                componentState.update(newComponent);
				System.err.println(loopContext.getRuntimeContext().getIndexOfThisSubtask()+ "> " + "STEP " + loopContext.getKey() + " updated component state: " +componentState.value());
                //send new component around
                for (long neighbor : neighborsState.get()) {
                    out.collect(Either.Left(new Tuple2<>(neighbor, newComponent)));
					System.err.println(loopContext.getRuntimeContext().getIndexOfThisSubtask()+ "> " + "STEP message sent from " + loopContext.getKey() + " to " + neighbor + ": " + newComponent);
                }
            }


        }

        @Override
        public void onTermination(LoopContext<Long> loopContext,
            Collector<Either<Tuple2<Long, Long>, Tuple2<Long, Long>>> out) throws Exception {

			System.err.println(loopContext.getRuntimeContext().getIndexOfThisSubtask()+ "> " + "ON TERMINATION: " + loopContext.getKey() + ", component: " + componentState.value());

			checkAndInitState(loopContext);

			out.collect(Either.Right(new Tuple2<>(loopContext.getKey(), componentState.value())));
		}

        private void checkAndInitState(LoopContext<Long> ctx) {
            if (neighborsState == null) {
            	neighborsState = ctx.getRuntimeContext().getListState(new ListStateDescriptor<Long>("neighbors", LongSerializer.INSTANCE));
            }

            if(componentState == null) {
            	componentState = ctx.getRuntimeContext().getState(new ValueStateDescriptor<Long>("components", LongSerializer.INSTANCE));
			}
        }
    }
}
