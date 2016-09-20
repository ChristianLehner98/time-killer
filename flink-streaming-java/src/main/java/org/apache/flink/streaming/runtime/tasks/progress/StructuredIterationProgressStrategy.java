package org.apache.flink.streaming.runtime.tasks.progress;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class StructuredIterationProgressStrategy implements StreamIterationProgressStrategy {
	private int maxIterations;

	private StructuredIterationProgressStrategy() {}
	public StructuredIterationProgressStrategy(int maxIterations) {
		this.maxIterations = maxIterations;
	}

	public void observe(StreamRecord record) {}

	public Watermark getNextWatermark(Watermark watermark) {
		if (watermark.getTimestamp() == Long.MAX_VALUE) return null;

		if (watermark.getTimestamp() == maxIterations-1) {
			return new Watermark(watermark.getContext(), Long.MAX_VALUE);
		}
		return watermark;
	}
}
