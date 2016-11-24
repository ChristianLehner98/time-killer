package org.apache.flink.streaming.api.functions.windowing;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

import java.io.Serializable;

@Public
public interface CoWindowTerminateFunction<IN,F_IN,OUT,F_OUT,KEY,W_IN extends Window> extends TerminationFunction, Serializable {
	void apply1(KEY key, W_IN window, Iterable<IN> input, Collector<Either<F_OUT,OUT>> out) throws Exception;

	// TODO add i?
	void apply2(KEY key, TimeWindow window, Iterable<F_IN> input, Collector<Either<F_OUT,OUT>> out) throws Exception;

	boolean terminate(int i);
	void onTermination(int i, Collector<Either<F_OUT,OUT>> out) throws Exception;
}
