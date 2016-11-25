package org.apache.flink.streaming.api.datastream;

public interface FeedbackBuilder<R> {
	<F,K> KeyedStream<F,K> feedback(DataStream<R> input);
}
