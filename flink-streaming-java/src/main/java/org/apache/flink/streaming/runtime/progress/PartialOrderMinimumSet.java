package org.apache.flink.streaming.runtime.progress;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class PartialOrderMinimumSet {
	private Set<List<Long>> elements = new HashSet<>();
	private int timestampsLength;

	public PartialOrderMinimumSet(int length) {
		this.timestampsLength = length;
	}

	public PartialOrderMinimumSet(PartialOrderMinimumSet other) {
		this.timestampsLength = other.timestampsLength;
		this.elements = new HashSet<>(other.elements);
	}

	public boolean update(List<Long> newElement) {
		if(newElement.size() != timestampsLength) {
			throw new RuntimeException("Length mismatch");
		}

		boolean changed = false;

		Iterator<List<Long>> it = elements.iterator();
		boolean add = true;
		while (it.hasNext()) {
			switch(partialCmp(newElement, it.next())) {
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

	public Set<List<Long>> getElements() {
		return elements;
	}

	public int getTimestampsLength() {
		return timestampsLength;
	}

	private enum PartialComparison {
		LESS, EQUAL, GREATER, INCOMPARABLE
	}

	private PartialComparison partialCmp(List<Long> ts1, List<Long> ts2) {
		boolean lessEquals = true;
		boolean greaterEquals = true;
		for(int i=0; i<ts1.size(); ++i) {
			if(ts1.get(i) > ts2.get(i)) {
				lessEquals = false;
			}
			if (ts1.get(i) < ts2.get(i)) {
				greaterEquals = false;
			}
		}
		if(lessEquals && greaterEquals) return PartialComparison.EQUAL;
		if(lessEquals && !greaterEquals) return PartialComparison.LESS;
		if(!lessEquals && greaterEquals) return PartialComparison.GREATER;
		return PartialComparison.INCOMPARABLE;
	}
}
