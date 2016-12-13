package org.apache.flink.streaming.runtime.tasks.progress;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public abstract class StreamIterationProgressStrategy {
	public abstract StreamRecord observe(StreamRecord record);
	public abstract Watermark getNextWatermark(Watermark watermark);
}
