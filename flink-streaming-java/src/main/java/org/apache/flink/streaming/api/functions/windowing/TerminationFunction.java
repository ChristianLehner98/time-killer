package org.apache.flink.streaming.api.functions.windowing;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.util.Collector;

import java.io.Serializable;

@Public
public interface TerminationFunction<OUT> extends Function, Serializable {
	boolean terminate(int i);
	void onTermination(int i, Collector<OUT> out) throws Exception;
}
