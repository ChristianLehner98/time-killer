package org.apache.flink.streaming.examples.iteration.issuedemonstrations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
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
 * This is a totally useless demonstration program that shows some behaviour that is unintuitive to me
 *
 * 1. The onTemination function is called for every new Watermark, even if no Window terminates at hat timestamp
 *
 * 2. Windows that don't have a Watermark for their exact end seem to be ignored completely
 *
 * This would mean that the WindowSize and the sending of Watermarks from the source have to be coordinated
 * to each other, which seems not exactly like desired behavior
 */

public class WatermarksExample {

	private static Logger LOG = LoggerFactory.getLogger(SaveAndReadPersistentStateExample.class);
	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static void main(String args[]) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



    }

    public WatermarksExample() throws Exception {
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		//parallelism 1 here only for testing purposes
		env.setParallelism(1);

		DataStream<Long> input = env.addSource(new OTExampleSource());

		KeyedStream<Long, Long> keyedInput = input.keyBy(new KeySelector<Long, Long>() {
			@Override
			public Long getKey(Long element) throws Exception {
				return element;
			}
		});

		//window size is 1 second
		WindowedStream<Long, Long, TimeWindow> windowedInput = keyedInput.timeWindow(Time.milliseconds(1000));


		DataStream<Long> results = windowedInput.iterateSync(new OTExampleWindowLoopFunction(),
			new FixpointIterationTermination(), new FeedbackBuilder<Long, Long>() {
				@Override
				public KeyedStream<Long, Long> feedback(DataStream<Long> dataStream) {
					return dataStream.keyBy(new KeySelector<Long, Long>() {
						@Override
						public Long getKey(Long element) throws Exception {
							return element;
						}
					});
				}
			}, BasicTypeInfo.LONG_TYPE_INFO);

	}

	public void run() throws Exception {
		System.err.println(env.getExecutionPlan());
		env.execute("Watermark problems Issue Demonstration");
	}

    private static class OTExampleSource extends RichSourceFunction<Long> {


        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            for(long timestamp = 0; timestamp <= 10000; timestamp += 400) {
                ctx.collectWithTimestamp(timestamp, timestamp);
				System.err.println("element generated at source, timestamp: " + timestamp);
				LOG.debug("element generated at source, timestamp: {}", timestamp);
                //watermarks are going to be sent every 400 milliseconds, this means only every second window will
				//have a watermark corresponding exactly to its endpoint
                ctx.emitWatermark(new Watermark(timestamp - 1));
                Thread.sleep(400);
            }
        }

        @Override
        public void cancel() {

        }
    }

    private static class OTExampleWindowLoopFunction implements WindowLoopFunction<Long, Long, Long, Long, Long, TimeWindow> {

		private Map<List<Long>, List<Long>> elementsPerContext = new HashMap<>();

    	@Override
        public void entry(LoopContext<Long> loopContext, Iterable<Long> iterable,
            Collector<Either<Long, Long>> collector) throws Exception {

            //trace all elements received by the entry function. The key is equal to the timestamp. Watermarks are sent
			//for 1ms before very timestamp
            System.err.println((loopContext.getRuntimeContext().getIndexOfThisSubtask() + 1) + "> " + "ENTRY with timestamp: " + iterable.iterator().next());
            LOG.debug("{}> ENTRY with timestamp: {}", loopContext.getRuntimeContext().getIndexOfThisSubtask() + 1,
				iterable.iterator().next());

            List<Long> elements = elementsPerContext.get(loopContext.getContext());
            if(elements == null) {
            	elements = new ArrayList<>();
            	elementsPerContext.put(loopContext.getContext(), elements);
			}
			elements.add(iterable.iterator().next());

        }

        @Override
        public void step(LoopContext<Long> loopContext, Iterable<Long> iterable,
            Collector<Either<Long, Long>> collector) throws Exception {
            //not necessary to see the point of the example
        }

        @Override
        public void onTermination(LoopContext<Long> loopContext, Collector<Either<Long, Long>> collector)
            throws Exception {

			List<Long> elements = elementsPerContext.getOrDefault(loopContext.getContext(), new ArrayList<>());

            //trace all calls of the OnTermination function
            System.err.println((loopContext.getRuntimeContext().getIndexOfThisSubtask() + 1) + "> " + "ON_TERMINATION :: " + loopContext.getContext() + ", elements in this window: "
				+ elements);
            LOG.debug("{}> ON_TERMINATION :: {}, elements in this window: {}",
				loopContext.getRuntimeContext().getIndexOfThisSubtask() + 1, loopContext.getContext(), elements);

        }
    }
}
