package org.apache.flink.streaming.runtime.progress;


import java.util.*;

// called Mutable Antichain in timely-dataflow
public class PartialOrderProgressAggregator {
	private CountMap<List<Long>> occurences; // occurence count of each time
	private Map<List<Long>, Integer> precedents = new HashMap<>(); // counts number of distinct times in occurences strictly less than element
	private Set<List<Long>> frontier = new HashSet<>(); // the set of times with precedent count == 0

	private int elementLength;

	public PartialOrderProgressAggregator(int elementLength) {
		this.elementLength = elementLength;
	}

	public Set<List<Long>> getFrontier() {
		return frontier;
	}

	public boolean updateAll(Map<List<Long>, Integer> elements) {
		boolean frontierChanged = false;
		for(Map.Entry<List<Long>, Integer> element : elements.entrySet()) {
			if(update(element.getKey(), element.getValue())) frontierChanged = true;
		}
		return frontierChanged;
	}

	public boolean update(List<Long> timestamp, int delta) {
		if(timestamp.size() != elementLength) throw new RuntimeException("Trying to add an element of wrong size to PartialOrderProgressAggregator");
		if(delta == 0) return false;

		boolean frontierChanged = false;
		int newValue = occurences.update(timestamp, delta);
		int oldValue = newValue - delta;

		if(oldValue <= 0 && newValue > 0) { // from negative or 0 to positive
			int precededBy = 0;

			for(Map.Entry<List<Long>, Integer> precedent : precedents.entrySet()) {
				// General Goal: add timestamp to all datastructures and change other entries accordingly

				PartialComparison cmp = partialCmp(timestamp, precedent.getKey());

				// Incomparable not interesting for precedents list
				if(cmp == PartialComparison.LESS) {
					if(precedent.getValue() == 0) {
						// the precedent is currently in the frontier and should be removed from it
						frontier.remove(precedent.getKey());
						frontierChanged = true;
					}
					// increment precedents of 'precedent' by one (the newly added timestamp)
					precedent.setValue(precedent.getValue() + 1);
				} else if (cmp == PartialComparison.GREATER) {
					// 'precedent' is lower, so we increment our precendentValue
					precededBy += 1;
				} else if(cmp == PartialComparison.EQUAL) {
					throw new RuntimeException("Shouldn't happen!");
				}
			}
			// add new timestamp to precendents and eventually to frontier
			precedents.put(timestamp, precededBy);
			if(precededBy == 0) {
				frontier.add(timestamp);
				frontierChanged = true;
			}
		} else if(oldValue > 0 && newValue <=0) { // from positive to zero or negative
			// General Goal: remove timestamp from all datastructures and change other entries accordingly

			for(Map.Entry<List<Long>, Integer> precedent : precedents.entrySet()) {
				PartialComparison cmp = partialCmp(timestamp, precedent.getKey());

				if (cmp == PartialComparison.LESS) {
					// 'precedent' is greater and since this timestamp is "leaving" its precededBy-Value has to be decremented
					precedent.setValue(precedent.getValue() - 1);
					if(precedent.getValue() == 0) {
						// the precedent should now be in the frontier
						frontier.add(precedent.getKey());
						frontierChanged = true;
					}
				}
			}
			if(frontier.remove(timestamp)) {
				// frontier contained our timestamp and is removed
				frontierChanged = true;
			}
			precedents.remove(timestamp);
		}
		return frontierChanged;
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
