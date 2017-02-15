package org.apache.flink.runtime.progress;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class PartialOrderMinimumSet implements Serializable {
	private Set<List<Long>> elements = new HashSet<>();
	private int timestampsLength;

	public PartialOrderMinimumSet(int length) {
		this.timestampsLength = length;
	}

	public boolean updateAll(Set<List<Long>> elements) {
		boolean changed = false;
		for(List<Long> element : elements) {
			changed = changed || update(element);
		}
		return changed;
	}

	public boolean update(List<Long> newElement) {
		if(newElement.size() != timestampsLength) {
			throw new RuntimeException("Length mismatch");
		}

		boolean changed = false;

		Iterator<List<Long>> it = elements.iterator();
		boolean add = true;
		while (it.hasNext()) {
			switch(PartialOrderComparator.partialCmp(newElement, it.next())) {
				case LESS:
					it.remove();
					changed = true;
				case GREATER:
					add = false;
			}
		}
		if(add) {
			elements.add(newElement);
			changed = true;
		}

		return changed;
	}

	public void incrementTimestamps(int level) {
		for(List<Long> timestamp : elements) {
			timestamp.set(level, timestamp.get(level) + 1);
		}
	}

	public Set<List<Long>> getElements() {
		return elements;
	}

	public int getTimestampsLength() {
		return timestampsLength;
	}
}
