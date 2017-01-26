package org.apache.flink.runtime.progress;

import java.util.List;

public class PartialOrderComparator {
	public enum PartialComparison {
		LESS, EQUAL, GREATER, INCOMPARABLE
	}

	public static PartialComparison partialCmp(List<Long> ts1, List<Long> ts2) {
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
