package org.apache.flink.runtime.progress.messages;

public class ProgressRegistration {
	private int operatorId;
	private int scopeLevel;
	private int parallelism;

	public ProgressRegistration(int operatorId, int scopeLevel, int parallelism) {
		this.operatorId = operatorId;
		this.scopeLevel = scopeLevel;
		this.parallelism = parallelism;
	}

	public Integer getOperatorId() {
		return operatorId;
	}
	public Integer getScopeLevel() {
		return scopeLevel;
	}
	public Integer getParallelism() {
		return parallelism;
	}

	@Override
	public String toString() {
		return "ProgressRegistration (Op./ScopeLvl): (" + operatorId + "/" + scopeLevel;
	}
}
