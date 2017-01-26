package org.apache.flink.runtime.progress.messages;

import java.util.List;

public class ProgressNotificationRequest {
	Integer operatorId;
	private List<Long> timestamp;

	public ProgressNotificationRequest(Integer operatorId, List<Long> timestamp) {
		this.operatorId = operatorId;
		this.timestamp = timestamp;
	}

	public List<Long> getTimestamp() {
		return timestamp;
	}
	public Integer getOperatorId() {
		return operatorId;
	}

	public void setTimestamp(List<Long> timestamp) {
		this.timestamp = timestamp;
	}
}
