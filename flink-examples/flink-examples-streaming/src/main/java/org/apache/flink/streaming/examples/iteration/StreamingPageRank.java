package org.apache.flink.streaming.examples.iteration;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.examples.java.graph.util.PageRankData;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.FeedbackBuilder;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.CoWindowTerminateFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Serializable;
import java.util.*;

public class StreamingPageRank {
	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

	public static void main(String[] args) throws Exception {
		int numWindows = Integer.parseInt(args[0]);
		long windSize = Long.parseLong(args[1]);
		int parallelism = Integer.parseInt(args[2]);
		String outputDir = args[3];
		String inputDir = args.length > 4 ? args[4] : "";
		
		StreamingPageRank example = new StreamingPageRank(numWindows, windSize, parallelism, inputDir, outputDir);
		example.run();
	}

	/**
	 * TODO configure the windSize parameter and generalize the evaluation framework
	 * 
	 * @param numWindows
	 * @param windSize
	 * @param parallelism
	 * @throws Exception
	 */
	public StreamingPageRank(int numWindows, long windSize, int parallelism, String inputDir, String outputDir) throws Exception {
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(parallelism);

		SourceFunction source;
		if(!inputDir.equals("")) {
			source = new PageRankFileSource(numWindows, inputDir);
		} else {
			source = new PageRankSource(numWindows);
		}
		DataStream<Tuple2<Long,List<Long>>> inputStream = env.addSource(source);
		inputStream
			.keyBy(0)
			.timeWindow(Time.milliseconds(1))
			.iterateSyncFor(10,
				new MyCoWindowTerminateFunction(),
				new MyFeedbackBuilder(),
				new TupleTypeInfo<Tuple2<Long, Double>>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO))
			.print();
		env.getConfig().setExperimentConstants(numWindows, windSize, outputDir);
	}

	protected void run() throws Exception {
		env.execute("Streaming Sync Iteration Example");
	}

	private static class MyFeedbackBuilder implements FeedbackBuilder {
		@Override
		public KeyedStream feedback(DataStream input) {
			return input.keyBy(0);
		}
	}

	private static class PageRankSource extends RichParallelSourceFunction<Tuple2<Long,List<Long>>> {
		private int numberOfGraphs;

		public PageRankSource(int numberOfGraphs) {
			this.numberOfGraphs = numberOfGraphs;
		}

		@Override
		public void run(SourceContext<Tuple2<Long, List<Long>>> ctx) {
			int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
			int parallelTask = getRuntimeContext().getIndexOfThisSubtask();

			for(int i=0; i<numberOfGraphs; i++) {
				for(Tuple2<Long,List<Long>> entry : getAdjancencyList()) {
					if(entry.f0 % parallelism == parallelTask) {
						ctx.collectWithTimestamp(entry, i);
					}
				}
				ctx.emitWatermark(new Watermark(i));
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


	private static class PageRankFileSource extends RichParallelSourceFunction<Tuple2<Long,List<Long>>> {
		private int numberOfGraphs;
		private String directory;

		public PageRankFileSource(int numberOfGraphs, String directory) throws Exception{
			this.numberOfGraphs = numberOfGraphs;
			this.directory = directory;
		}

		@Override
		public void run(SourceContext<Tuple2<Long, List<Long>>> ctx) throws Exception {
			String path = directory + "/" + getRuntimeContext().getNumberOfParallelSubtasks() + "/part-" + getRuntimeContext().getIndexOfThisSubtask();
			for(int i=0; i<numberOfGraphs; i++) {
				BufferedReader fileReader = new BufferedReader(new FileReader(path));
				String line;
				while( (line = fileReader.readLine()) != null) {
					String[] splitLine = line.split(" ");
					Long node = Long.parseLong(splitLine[0]);
					List<Long> neighbours = new LinkedList<>();
					for(int neighbouri=1; neighbouri<splitLine.length; ++neighbouri) {
						neighbours.add(Long.parseLong(splitLine[neighbouri]));
					}
					ctx.collectWithTimestamp(new Tuple2<>(node, neighbours), i);
				}
				ctx.emitWatermark(new Watermark(i));
			}
		}

		@Override
		public void cancel() {}
	}

	private static class MyCoWindowTerminateFunction implements CoWindowTerminateFunction<Tuple2<Long, List<Long>>, Tuple2<Long, Double>, Tuple2<Long,Double>, Tuple2<Long, Double>, Tuple, TimeWindow>, Serializable {
		Map<List<Long>,Map<Long, List<Long>>> neighboursPerContext = new HashMap<>();
		Map<List<Long>,Map<Long,Double>> pageRanksPerContext = new HashMap<>();

		public List<Long> getNeighbours(List<Long> timeContext, Long pageID) {
			return neighboursPerContext.get(timeContext).get(pageID);
		}

		@Override
		public void entry(Tuple key, TimeWindow win, Iterable<Tuple2<Long, List<Long>>> iterable, Collector<Either<Tuple2<Long, Double>, Tuple2<Long,Double>>> collector) {
			//System.out.println(win.getTimeContext() + " " + win.getStart());
			Map<Long, List<Long>> neighbours = neighboursPerContext.get(win.getTimeContext());
			if(neighbours == null) {
				neighbours = new HashMap<>();
				neighboursPerContext.put(win.getTimeContext(), neighbours);
			}

			Map<Long,Double> pageRanks = pageRanksPerContext.get(win.getTimeContext());
			if(pageRanks == null) {
				pageRanks = new HashMap<>();
				pageRanksPerContext.put(win.getTimeContext(), pageRanks);
			}

			for(Tuple2<Long,List<Long>> entry : iterable) {
				// save neighbours to local state
				neighbours.put(entry.f0, entry.f1);

				// save page rank to local state
				pageRanks.put(entry.f0, 1.0);

				// send rank into feedback loop
				collector.collect(new Either.Left(new Tuple2<>(entry.f0, 1.0)));
			}
		}

		@Override
		public void step(Tuple key, TimeWindow win, Iterable<Tuple2<Long, Double>> iterable, Collector<Either<Tuple2<Long, Double>, Tuple2<Long,Double>>> collector) {
			//System.out.println(win.getTimeContext() + " " + win.getStart());
			Map<Long,Double> summed = new HashMap<>();
			for(Tuple2<Long,Double> entry : iterable) {
				Double current = summed.get(entry.f0);
				if(current == null) {
					summed.put(entry.f0, entry.f1);
				}
				else {
					summed.put(entry.f0, current+entry.f1);
				}
			}

			for(Map.Entry<Long,Double> entry : summed.entrySet()) {
				List<Long> neighbourIDs = getNeighbours(win.getTimeContext(),entry.getKey());
				Double currentRank = entry.getValue();

				// update current rank
				pageRanksPerContext.get(win.getTimeContext()).put(entry.getKey(), currentRank);

				// generate new ranks for neighbours
				Double rankToDistribute = currentRank / (double) neighbourIDs.size();

				for(Long neighbourID : neighbourIDs) {
					collector.collect(new Either.Left(new Tuple2<>(neighbourID, rankToDistribute)));
				}
			}
		}

		@Override
		public void onTermination(List<Long> timeContext, Collector<Either<Tuple2<Long, Double>, Tuple2<Long, Double>>> out) {
			for(Map.Entry<Long,Double> rank : pageRanksPerContext.get(timeContext).entrySet()) {
				out.collect(new Either.Right(new Tuple2(rank.getKey(), rank.getValue())));
			}
		}
	}
}
