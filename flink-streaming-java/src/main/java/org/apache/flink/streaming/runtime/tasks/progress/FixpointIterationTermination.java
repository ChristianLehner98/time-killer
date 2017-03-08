package org.apache.flink.streaming.runtime.tasks.progress;

import 	org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;

public class FixpointIterationTermination implements StreamIterationTermination {
	private Map<List<Long>, Boolean> convergedTracker = new HashMap<>();
	private Set<List<Long>> done = new HashSet<>();
	protected static final Logger LOG = LoggerFactory.getLogger(FixpointIterationTermination.class);

	public boolean terminate(List<Long> timeContext) {
		return done.contains(timeContext);
	}

	public void observeRecord(StreamRecord record) {
		done.remove(record.getContext()); // if this partition is "back alive"
		convergedTracker.put(record.getContext(), false);
	}

	public void observeWatermark(Watermark watermark) {
		//if(convergedTracker.get(watermark.getContext()) == null) convergedTracker.put(watermark.getContext(), false);
		if(convergedTracker.get(watermark.getContext())) {
			done.add(watermark.getContext());
		} else {
			convergedTracker.put(watermark.getContext(), true);
		}
	}
}
