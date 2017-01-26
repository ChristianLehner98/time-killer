package org.apache.flink.runtime.progress;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProgressUpdate {
	private Map<Tuple2<Integer,List<Long>>, Integer> countmap = new HashMap<>();
	private Integer operatorId;

	public ProgressUpdate(Integer operatorId) {
		this.operatorId = operatorId;
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

	public Map<Tuple2<Integer,List<Long>>, Integer> get() {
		return countmap;
	}
	public Integer getOperatorId() { return operatorId; }
	public void setOperatorId(Integer operatorId) {
		this.operatorId = operatorId;
	}

	public int get(Tuple2<Integer,List<Long>> pointstamp) {
		Integer currentValue = countmap.get(pointstamp);
		if(currentValue == null) return 0;
		return currentValue;
	}
}
