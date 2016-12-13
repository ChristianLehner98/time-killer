package org.apache.flink.streaming.api.functions.windowing;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.List;

@Public
public interface TerminationFunction<OUT> extends Function, Serializable {
	void onTermination(List<Long> timeContext, Collector<OUT> out) throws Exception;
}
