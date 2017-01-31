package org.apache.flink.runtime.progress.messages;

import java.util.List;

public class ProgressNotification {
	private List<Long> timestamp;
	private boolean done = false;

	public ProgressNotification(List<Long> timestamp) {
		this.timestamp = timestamp;
	}

	public void setTimestamp(List<Long> timestamp) {
		this.timestamp = timestamp;
	}
	public List<Long> getTimestamp() {
		return timestamp;
	}

	public void setDone() {
		done = true;
	}
	public boolean isDone() {
		return done;
	}
}
