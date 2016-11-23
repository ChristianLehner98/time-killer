package org.apache.flink.api.common.functions;

import org.apache.flink.annotation.Public;
import org.apache.flink.util.Collector;

import java.io.Serializable;

@Public
public interface TerminationFunction<T> extends Function, Serializable {
	void onTermination(Collector<T> out);
}
