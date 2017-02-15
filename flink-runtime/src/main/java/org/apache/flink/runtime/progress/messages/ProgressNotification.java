package org.apache.flink.runtime.progress.messages;

import java.util.List;

public class ProgressNotification {
	private List<Long> timestamp;
	private boolean done = false;

	public ProgressNotification(List<Long> timestamp, boolean done) {
		this.timestamp = timestamp;
		this.done = done;
	}

	public void setTimestamp(List<Long> timestamp) {
		this.timestamp = timestamp;
	}
	public List<Long> getTimestamp() {
		return timestamp;
	}

	public boolean isDone() {
		return done;
	}

	@Override
	public String toString() {
		return "ProgressNotification (Ts/Done): " + timestamp + "/" + done;
	}
}
