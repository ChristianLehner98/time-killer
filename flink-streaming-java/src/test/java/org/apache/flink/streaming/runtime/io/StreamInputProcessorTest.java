package org.apache.flink.streaming.runtime.io;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ResultPartitionWriter.class})
@PowerMockIgnore({"javax.management.*", "com.sun.jndi.*"})
public class StreamInputProcessorTest extends TestLogger{

	@Test
	public void testWatermarkHandling() throws Exception {
		final OneInputStreamTask<String, String> mapTask = new OneInputStreamTask<String, String>();
		final OneInputStreamTaskTestHarness<String, String> testHarness = new OneInputStreamTaskTestHarness<String, String>(mapTask, 1, 3, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

		StreamConfig streamConfig = testHarness.getStreamConfig();
		StreamMap<String, String> mapOperator = new StreamMapInIteration(new StreamInputProcessorTest.IdentityMap());
		streamConfig.setStreamOperator(mapOperator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<Object>();

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		List<Long> context0 = new LinkedList<>();
		context0.add(new Long(0));
		List<Long> context1 = new LinkedList<>();
		context1.add(new Long(1));

		testHarness.processElement(new Watermark(context0, 5), 0, 0);
		testHarness.processElement(new Watermark(context1, 4), 0, 1);
		testHarness.processElement(new Watermark(context1, 3), 0, 0);
		testHarness.processElement(new Watermark(context0, 2), 0, 2);

		// now the output should still be empty
		testHarness.waitForInputProcessing();
		ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
		for(Object obj : output) {
			System.out.println(obj);
		}
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, output);

		testHarness.processElement(new Watermark(context0, 2), 0, 1); // output should be (0,2)
		testHarness.processElement(new Watermark(context1, 6), 0, 2); // output should be (1,3)
		testHarness.processElement(new Watermark(context1, 7), 0, 0); // output should be (1,4)

		expectedOutput.add(new Watermark(context0, 2));
		expectedOutput.add(new Watermark(context1, 3));
		expectedOutput.add(new Watermark(context1, 4));
		testHarness.waitForInputProcessing();
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}

	private static class IdentityMap extends RichMapFunction<String,String> {
		
		private static final long serialVersionUID = 1L;

		@Override
		public String map(String value) throws Exception {
			return value;
		}
	}

	private static class StreamMapInIteration extends StreamMap<String,String> {
		public StreamMapInIteration(MapFunction<String,String> mapper) {
			super(mapper);
		}

		@Override
		public int getContextLevel() {
			return 1;
		}
	}
}


