package org.apache.flink.runtime.progress.messages;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProgressUpdate implements Serializable {
	private Map<Tuple3<Integer,List<Long>,Boolean>, Integer> countmap = new HashMap<>();

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
		countmap.put(pointstamp, newValue);
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

	@Override
	public String toString() {
		return "ProgressUpdate: " + countmap;
	}
}
