package org.apache.flink.runtime.progress.messages;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ProgressUpdate implements Serializable {
	private Map<Tuple3<Integer,List<Long>,Boolean>, Integer> countmap = new HashMap<>();
	private Integer instanceId;

	public ProgressUpdate() {}
	public ProgressUpdate(ProgressUpdate other) {
		this.countmap = new HashMap<>(other.countmap);
	}
	public ProgressUpdate(ProgressUpdate other, Integer instanceId) {
		this.countmap = new HashMap<>(other.countmap);
		this.instanceId = instanceId;
	}

	public void mergeIn(ProgressUpdate other) {
		for(Map.Entry<Tuple3<Integer,List<Long>,Boolean>, Integer> entry : other.getEntries().entrySet()) {
			update(entry.getKey().f0, new LinkedList<>(entry.getKey().f1), entry.getKey().f2, entry.getValue());
		}
	}

	public int update(List<Long> element, int delta) {
		// in case we only want to track one operator anyways, we just fill in a 0
		return update(0, element, delta);
	}

	public int update(Integer operatorId, List<Long> element, int delta) {
		return update(operatorId, element, false, delta);
	}

	public int update(Integer operatorId, List<Long> element, boolean isCapability, int delta) {
		Tuple3<Integer, List<Long>, Boolean> pointstamp = new Tuple3<>(operatorId, element, isCapability);
		if(delta == 0) return get(pointstamp);

		Integer newValue = countmap.get(pointstamp);
		if(newValue == null) {
			newValue = delta;
		} else {
			newValue += delta;
		}
		if(newValue != 0) {
			countmap.put(pointstamp, newValue);
		} else {
			countmap.remove(pointstamp);
		}
		return newValue;
	}

	public Map<Tuple3<Integer,List<Long>,Boolean>, Integer> getEntries() {
		return countmap;
	}

	public int get(Tuple3<Integer,List<Long>,Boolean> pointstamp) {
		Integer currentValue = countmap.get(pointstamp);
		if(currentValue == null) return 0;
		return currentValue;
	}

	public void clear() {
		countmap = new HashMap<>();
	}

	@Override
	public String toString() {
		return "ProgressUpdate: " + countmap;
	}

	public Integer getInstanceId() {
		return instanceId;
	}
}
