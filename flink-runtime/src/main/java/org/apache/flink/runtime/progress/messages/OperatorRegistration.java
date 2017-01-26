package org.apache.flink.runtime.progress.messages;

public class OperatorRegistration {
	private Integer operatorId;
	private Integer scopeLevel;

	public OperatorRegistration(Integer operatorId, Integer scopeLevel) {
		this.operatorId = operatorId;
		this.scopeLevel = scopeLevel;
	}

	public Integer getOperatorId() {
		return operatorId;
	}
	public Integer getScopeLevel() {
		return scopeLevel;
	}
}
