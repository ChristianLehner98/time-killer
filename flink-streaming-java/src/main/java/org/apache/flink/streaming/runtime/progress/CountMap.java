package org.apache.flink.streaming.runtime.progress;

import java.util.HashMap;
import java.util.Map;

public class CountMap<T> {
	private Map<T, Integer> countmap = new HashMap<>();

	public int update(T element, int delta) {
		if(delta == 0) return get(element);

		Integer newValue = countmap.get(element);
		if(newValue == null) {
			newValue = delta;

		} else {
			newValue += delta;
		}
		countmap.put(element, newValue);
		return newValue;
	}

	public int get(T element) {
		Integer currentValue = countmap.get(element);
		if(currentValue == null) return 0;
		return currentValue;
	}
}
