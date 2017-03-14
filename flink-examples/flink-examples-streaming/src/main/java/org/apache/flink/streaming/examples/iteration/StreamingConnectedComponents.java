package org.apache.flink.streaming.examples.iteration;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.examples.java.graph.util.ConnectedComponentsData;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.FeedbackBuilder;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.CoWindowTerminateFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.tasks.progress.FixpointIterationTermination;
import org.apache.flink.streaming.runtime.tasks.progress.StructuredIterationTermination;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class StreamingConnectedComponents {
	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

	public static void main(String[] args) throws Exception {

		StreamingConnectedComponents example = new StreamingConnectedComponents();
		example.run();
	}

	public StreamingConnectedComponents() throws Exception {
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(2);

		DataStream<Tuple2<Long,Set<Long>>> inputStream = env.addSource(new ConnectedComponentsSource(1));
		inputStream
			.keyBy(0)
			.timeWindow(Time.milliseconds(1))
			.iterateSync(new MyCoWindowTerminateFunction(),
				new FixpointIterationTermination(),
				new MyFeedbackBuilder(),
				new TupleTypeInfo<Tuple2<Long, Long>>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO))
			.print();
		System.out.println(env.getExecutionPlan());
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

	private static class ConnectedComponentsSource extends RichParallelSourceFunction<Tuple2<Long,Set<Long>>> {
		private int numberOfGraphs;

		public ConnectedComponentsSource(int numberOfGraphs) {
			this.numberOfGraphs = numberOfGraphs;
		}

		@Override
		public void run(SourceContext<Tuple2<Long, Set<Long>>> ctx) {
			int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
			int parallelTask = getRuntimeContext().getIndexOfThisSubtask();

			for(int i=0; i<numberOfGraphs; i++) {
				for(Tuple2<Long,Set<Long>> entry : getAdjancencyList()) {
					if(entry.f0 % parallelism == parallelTask) {
						ctx.collectWithTimestamp(entry, i);
					}
				}
			}
		}

		@Override
		public void cancel() {}

		private List<Tuple2<Long,Set<Long>>> getAdjancencyList() {
			Map<Long,Set<Long>> edges = new HashMap<>();
			for(Object[] e : ConnectedComponentsData.EDGES) {
				Set<Long> currentVertexEdges = edges.get((Long) e[0]);
				if(currentVertexEdges == null) {
					currentVertexEdges = new HashSet<>();
					edges.put((Long) e[0], currentVertexEdges);
				}
				currentVertexEdges.add((Long) e[1]);

				Set<Long> targetVertexEdges = edges.get((Long) e[1]);
				if(targetVertexEdges == null) {
					targetVertexEdges = new HashSet<>();
					edges.put((Long) e[1], targetVertexEdges);
				}
				targetVertexEdges.add((Long) e[0]);
			}
			List<Tuple2<Long,Set<Long>>> input = new LinkedList<>();
			for(Map.Entry<Long, Set<Long>> entry : edges.entrySet()) {
				input.add(new Tuple2<>(entry.getKey(), entry.getValue()));
			}
			return input;
		}
	}

	private static class ConnectedComponentsFileSource extends RichParallelSourceFunction<Tuple2<Long,Set<Long>>> {
		private int numberOfGraphs;
		private BufferedReader fileReader;

		public ConnectedComponentsFileSource(int numberOfGraphs, String directory) throws Exception{
			this.numberOfGraphs = numberOfGraphs;
			String path = directory + "/" + getRuntimeContext().getIndexOfThisSubtask();
			fileReader = new BufferedReader(new FileReader(path));
		}

		@Override
		public void run(SourceContext<Tuple2<Long, Set<Long>>> ctx) throws Exception {
			for(int i=0; i<numberOfGraphs; i++) {
				String line;
				while( (line = fileReader.readLine()) != null) {
					String[] splitLine = line.split(" ");
					Long node = Long.parseLong(splitLine[0]);
					Set<Long> neighbours = new HashSet<>();
					for(int neighbouri=1; neighbouri<splitLine.length; ++neighbouri) {
						neighbours.add(Long.parseLong(splitLine[neighbouri]));
					}
					ctx.collectWithTimestamp(new Tuple2<>(node, neighbours), i);
				}
			}
		}

		@Override
		public void cancel() {}
	}

	private static class MyCoWindowTerminateFunction implements CoWindowTerminateFunction<Tuple2<Long, Set<Long>>, Tuple2<Long, Long>, Tuple2<Long,Long>, Tuple2<Long, Long>, Tuple, TimeWindow>, Serializable {
		Map<List<Long>,Map<Long, Set<Long>>> neighboursPerContext = new HashMap<>();
		Map<List<Long>,Map<Long, Long>> componentsPerContext = new HashMap<>();

		@Override
		public void entry(Tuple key, TimeWindow win, Iterable<Tuple2<Long, Set<Long>>> iterable, Collector<Either<Tuple2<Long, Long>, Tuple2<Long,Long>>> collector) {
			// save graph
			Map<Long, Set<Long>> neighbours = neighboursPerContext.get(win.getTimeContext());
			if(neighbours == null) {
				neighbours = new HashMap<>();
				neighboursPerContext.put(win.getTimeContext(), neighbours);
			}

			// init components
			Map<Long,Long> components = componentsPerContext.get(win.getTimeContext());
			if(components == null) {
				components = new HashMap<>();
				componentsPerContext.put(win.getTimeContext(), components);
			}

			for(Tuple2<Long,Set<Long>> entry : iterable) {
				// save neighbours to local state
				neighbours.put(entry.f0, entry.f1);

				// init component with node id
				components.put(entry.f0, Long.MAX_VALUE);

				// send (node id, component id) into feedback loop
				collector.collect(new Either.Left(new Tuple2<>(entry.f0, entry.f0)));
			}
		}

		@Override
		public void step(Tuple key, TimeWindow win, Iterable<Tuple2<Long, Long>> iterable, Collector<Either<Tuple2<Long, Long>, Tuple2<Long,Long>>> collector) {
			long currentNode = (long) ((Tuple1) key).f0;
			Map<Long,Set<Long>> neighbours = neighboursPerContext.get(win.getTimeContext());
			Map<Long,Long> components = componentsPerContext.get(win.getTimeContext());

			long min = Long.MAX_VALUE;
			for(Tuple2<Long,Long> entry : iterable) {
				if(entry.f1 < min) min = entry.f1;
			}

			if(min < components.get(currentNode)) {
				components.put(currentNode, min);
				for(Long neighbour : neighbours.get(currentNode)) {
					collector.collect(new Either.Left(new Tuple2<>(neighbour, min)));
				}
			}

		}

		@Override
		public void onTermination(List<Long> timeContext, Collector<Either<Tuple2<Long, Long>, Tuple2<Long, Long>>> out) {
			for(Map.Entry<Long,Long> component : componentsPerContext.get(timeContext).entrySet()) {
				out.collect(new Either.Right(new Tuple2(component.getKey(), component.getValue())));
			}
		}
	}
}
