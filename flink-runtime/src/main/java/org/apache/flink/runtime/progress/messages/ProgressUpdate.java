package org.apache.flink.runtime.progress.messages;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProgressUpdate implements Serializable {
	private Map<Tuple2<Integer,List<Long>>, Integer> countmap = new HashMap<>();

	public int update(List<Long> element, int delta) {
		// in case we only want to track one operator anyways, we just fill in a 0
		return update(0, element, delta);
	}

	public int update(Integer operatorId, List<Long> element, int delta) {
		Tuple2<Integer, List<Long>> pointstamp = new Tuple2<>(operatorId, element);
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

	public Map<Tuple2<Integer,List<Long>>, Integer> getEntries() {
		return countmap;
	}

	public int get(Tuple2<Integer,List<Long>> pointstamp) {
		Integer currentValue = countmap.get(pointstamp);
		if(currentValue == null) return 0;
		return currentValue;
	}
}
