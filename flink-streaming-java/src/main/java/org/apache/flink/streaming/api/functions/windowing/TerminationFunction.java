package org.apache.flink.streaming.api.functions.windowing;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;

@Public
public interface TerminationFunction<T, O> extends Function, Serializable {
	boolean terminate();
	void onTermination();
}
