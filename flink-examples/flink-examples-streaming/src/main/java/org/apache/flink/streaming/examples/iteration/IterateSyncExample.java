package org.apache.flink.streaming.examples.iteration;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.examples.java.graph.util.PageRankData;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.*;

public class IterateSyncExample {
	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

	public static void main(String[] args) throws Exception {

		IterateSyncExample example = new IterateSyncExample();
		example.run();
	}

	public IterateSyncExample() {
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Tuple2<Long,List<Long>>> inputStream = env.addSource(new PageRankSource(4));
		inputStream
			.keyBy(0)
			.timeWindow(Time.milliseconds(1))
			.iterateSync(new WindowLoopFunction<Tuple2<Long,List<Long>>, TimeWindow, Tuple2<Long,Long>, Tuple, Tuple2<Long,List<Long>>>() {
				public Tuple2<KeyedStream<Tuple2<Long,Long>, Tuple>, DataStream<Tuple2<Long, List<Long>>>> loop(IterativeWindowStream in) {
					ReduceFunction<>
					in.reduce();
				}
			});
	}

	protected void run() throws Exception {
		env.execute("Streaming Sync Iteration Example");
	}

	private class PageRankSource implements SourceFunction<Tuple2<Long,List<Long>>> {
		private int numberOfGraphs;

		public PageRankSource(int numberOfGraphs) {
			this.numberOfGraphs = numberOfGraphs;
		}

		@Override
		public void run(SourceContext<Tuple2<Long, List<Long>>> ctx) {
			for(int i=0; i<numberOfGraphs; i++) {
				for(Tuple2<Long,List<Long>> entry : getAdjancencyList()) {
					ctx.collectWithTimestamp(entry, i);
				}
				if(i!= 2) ctx.emitWatermark(new Watermark(i));
			}
		}

		@Override
		public void cancel() {}

		private List<Tuple2<Long,List<Long>>> getAdjancencyList() {
			Map<Long,List<Long>> edges = new HashMap<>();
			for(Object[] e : PageRankData.EDGES) {
				List<Long> currentVertexEdges = edges.get((Long) e[0]);
				if(currentVertexEdges == null) {
					currentVertexEdges = new LinkedList<>();
				}
				currentVertexEdges.add((Long) e[1]);
				edges.put((Long) e[0], currentVertexEdges);
			}
			List<Tuple2<Long,List<Long>>> input = new LinkedList<>();
			for(Map.Entry<Long, List<Long>> entry : edges.entrySet()) {
				input.add(new Tuple2(entry.getKey(), entry.getValue()));
			}
			return input;
		}
	}
}
