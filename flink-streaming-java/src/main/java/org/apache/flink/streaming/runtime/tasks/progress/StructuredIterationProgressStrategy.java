package org.apache.flink.streaming.runtime.tasks.progress;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class StructuredIterationProgressStrategy extends StreamIterationProgressStrategy {
	private int maxIterations;

	public StructuredIterationProgressStrategy(int maxIterations) {
		this.maxIterations = maxIterations;
	}

	public StreamRecord observe(StreamRecord record) {
		record.forwardTimestamp();
		return record;
	}

	public Watermark getNextWatermark(Watermark watermark) {
		if (watermark.getTimestamp() == Long.MAX_VALUE) return null;

		if (watermark.getTimestamp() == maxIterations-2) {
			return new Watermark(watermark.getContext(), Long.MAX_VALUE);
		}

		watermark.forwardTimestamp();
		return watermark;
	}
}
