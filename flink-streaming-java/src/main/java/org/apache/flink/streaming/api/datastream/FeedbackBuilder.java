package org.apache.flink.streaming.api.datastream;

import java.io.Serializable;

/**
 * The feedback edge. Transforms a Datastream<R> coming from the CoWindowTerminateFunction to a KeyedStream<F,K> that
 * is then windowed for BSP and feeded back to CoWindowTerminateFunction.
 *
 * Usages:
 * - simple: only keying (if all step functionality is done in CowindowTerminateFunction#step
 * - other: arbitrary operations on the feedback stream including nested iterations
 *
 * @param <R> 	  Intermediate type between CoWindowTerminateFunction and FeedbackBuilder
 * @param <F>	  Produced feedback type
 * @param <K>     Key of the feedback type
 */

public interface FeedbackBuilder<R> extends Serializable {
	<F,K> KeyedStream<F,K> feedback(DataStream<R> input);
}
