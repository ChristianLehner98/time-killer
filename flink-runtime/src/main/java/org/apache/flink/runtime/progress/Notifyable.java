package org.apache.flink.runtime.progress;

import java.util.List;

public interface Notifyable {
	void receiveProgressNotification(List<Long> timestamp, boolean done) throws Exception;
}
