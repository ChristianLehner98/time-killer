package org.apache.flink.streaming.runtime.tasks.progress;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public interface StreamIterationProgressStrategy {
	void observe(StreamRecord record);
	Watermark getNextWatermark(Watermark watermark);
}
