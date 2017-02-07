package org.apache.flink.runtime.progress.messages;

import java.util.List;

public class ProgressNotificationRequest {
	private Integer operatorId;
	private Integer instanceId;
	private List<Long> timestamp;
	private boolean done;

	public ProgressNotificationRequest(Integer operatorId, Integer instanceId, List<Long> timestamp, boolean done) {
		this.operatorId = operatorId;
		this.instanceId = instanceId;
		this.timestamp = timestamp;
	}

	public List<Long> getTimestamp() {
		return timestamp;
	}
	public Integer getOperatorId() {
		return operatorId;
	}
	public Integer getInstanceId() {
		return instanceId;
	}
	public boolean isDone() {
		return done;
	}

	public void setDone() {
		done = true;
	}
	public void setTimestamp(List<Long> timestamp) {
		this.timestamp = timestamp;
	}
}
