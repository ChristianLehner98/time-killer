package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.progress.FixpointProgressStrategy;
import org.apache.flink.streaming.runtime.tasks.progress.StructuredIterationTermination;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class StreamIterationTerminationTest {

	@Test
	public void fixpointProgressTest() {
		FixpointProgressStrategy progress = new FixpointProgressStrategy();



		List<Long> context0 = new LinkedList<>();
		context0.add(new Long(0));
		List<Long> context1 = new LinkedList<>();
		context1.add(new Long(1));

		Watermark watermark01 = new Watermark(context0, 1);
		Watermark watermark10 = new Watermark(context1, 0);

		// head should now track both context0 and context1
		// both are untracked before, so should be put out
		assert progress.getNextWatermark(watermark01).equals(watermark01);
		assert progress.getNextWatermark(watermark10).equals(watermark10);

		//make sure both contexts stay tracked
		progress.observe(new StreamRecord<>("", context0, 4));
		progress.observe(new StreamRecord<>("", context1, 2));

		Watermark watermark02 = new Watermark(context0, 2);
		Watermark watermark11 = new Watermark(context1, 1);

		// (0,1) came back as (0,2) => (0,2) should be put out
		assert progress.getNextWatermark(watermark02).equals(watermark02);
		// (1,0) came back as (1,1) => (1,1) should be put out
		assert progress.getNextWatermark(watermark11).equals(watermark11);

		// this time, only context 0 stays active
		progress.observe(new StreamRecord<>("", context0, 4));

		Watermark watermark03 = new Watermark(context0, 3);
		Watermark watermark12 = new Watermark(context1, 2);
		Watermark watermark1max = new Watermark(context1, Long.MAX_VALUE);

		// (0,2) came back as (0,3) => (0,3) should be put out
		assert progress.getNextWatermark(watermark03).equals(watermark03);
		// (1,1) came back as (1,2) but context 1 is finished
		// => (1,MAX_LONG) should be put out
		assert progress.getNextWatermark(watermark12).equals(watermark1max);
	}

	@Test
	public void structuredIterationProgressTest() {
		StructuredIterationTermination termination = new StructuredIterationTermination(2);

		List<Long> context0 = new LinkedList<>();
		context0.add(new Long(0));
		List<Long> context1 = new LinkedList<>();
		context1.add(new Long(1));

		Watermark watermark00 = new Watermark(context0, 0);
		Watermark watermark01 = new Watermark(context0, 1);
		Watermark watermark11 = new Watermark(context1, 1);

		termination.observeWatermark(watermark00);
		assert(termination.terminate(context0)) == false;
		termination.observeWatermark(watermark11);
		assert(termination.terminate(context1)) == true;
		termination.observeWatermark(watermark01);
		assert termination.terminate(context0) == true;
	}
}
