package org.apache.flink.runtime.progress;

import java.util.List;

public class ProgressNotification {
	private List<Long> timestamp;

	public ProgressNotification(List<Long> timestamp) {
		this.timestamp = timestamp;
	}

	public List<Long> getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(List<Long> timestamp) {
		this.timestamp = timestamp;
	}
}
