package org.apache.flink.streaming.api.datastream;

import java.io.Serializable;

public interface FeedbackBuilder<R> extends Serializable {
	<F,K> KeyedStream<F,K> feedback(DataStream<R> input);
}
