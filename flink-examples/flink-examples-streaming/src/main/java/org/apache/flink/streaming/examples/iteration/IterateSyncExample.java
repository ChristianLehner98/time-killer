package org.apache.flink.streaming.examples.iteration;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.examples.java.graph.util.PageRankData;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.CoWindowTerminateFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

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
			.iterateSync(new CoWindowTerminateFunction<Tuple2<Long, List<Long>>, Tuple2<Long, Double>, Tuple2<Long, List<Long>>, Tuple2<Long, Double>, Tuple, TimeWindow>() {
				Map<List<Long>,Map<Long, List<Long>>> neighboursPerContext = new HashMap<>();
				Map<List<Long>,Map<Long,Double>> pageRanksPerContext = new HashMap<>();

				public List<Long> getNeighbours(List<Long> timeContext, Long pageID) {
					return neighboursPerContext.get(timeContext).get(pageID);
				}

				@Override
				// TODO think about putting apply1 before apply2?
				public void apply1(Tuple key, TimeWindow win, Iterable<Tuple2<Long, List<Long>>> iterable, Collector<Either<Tuple2<Long, Double>, Tuple2<Long, List<Long>>>> collector) {
					Map<Long, List<Long>> neighbours = new HashMap<>();
					neighboursPerContext.put(win.getTimeContext(), neighbours);

					Map<Long,Double> pageRanks = new HashMap<>();
					pageRanksPerContext.put(win.getTimeContext(), pageRanks);

					for(Tuple2<Long,List<Long>> entry : iterable) {
						// save neighbours to local state
						neighbours.put(entry.f0, entry.f1);

						// save page rank to local state
						pageRanks.put(entry.f0, 1.0);

						// send rank into feedback loop
						collector.collect(new Either.Left(new Tuple2<>(entry.f0, 1.0)));
					}
				}

				// TUPLE key??
				@Override
				public void apply2(Tuple key, TimeWindow win, Iterable<Tuple2<Long, Double>> iterable, Collector<Either<Tuple2<Long, Double>, Tuple2<Long, List<Long>>>> collector) {
					for(Tuple2<Long,Double> entry : iterable) {
						List<Long> neighbourIDs = getNeighbours(win.getTimeContext(),entry.f0);
						Double currentRank = entry.f1;
						Double rankToDistribute = currentRank / (double) neighbourIDs.size();

						for(Long neighbourID : neighbourIDs) {
							collector.collect(new Either.Left(new Tuple2<>(neighbourID, rankToDistribute)));
						}
					}
				}

				@Override
				public boolean terminate(int i) {
					if (i < 20) return true;
					return false;
				}

				@Override
				public void onTermination(int i, Collector<Either<Tuple2<Long, Double>, Tuple2<Long, List<Long>>>> out) {
					// use getNeighbours()
				}
			}, new FeedbackBuilder() {
				@Override
				public KeyedStream feedback(DataStream input) {
					return input.keyBy(0).timeWindow(Time.milliseconds(1)).sum(0).keyBy(0);
				}
			}, new Tuple2<Long, Double>().getClass());
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
