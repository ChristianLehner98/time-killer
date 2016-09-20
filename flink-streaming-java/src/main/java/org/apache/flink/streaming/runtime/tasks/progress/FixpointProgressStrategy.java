package org.apache.flink.streaming.runtime.tasks.progress;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FixpointProgressStrategy implements StreamIterationProgressStrategy{
	private Map<List<Long>, Tuple2<Long, Boolean>> convergedTracker = new HashMap<>();

	public void observe(StreamRecord element) {
		List<Long> context = element.asRecord().getContext();
		Tuple2<Long, Boolean> converged = convergedTracker.get(context);
		if (converged != null) converged.f1 = false;
	}

	public Watermark getNextWatermark(Watermark watermark) {
		Long timestamp = watermark.getTimestamp();
		List<Long> context = watermark.getContext();

		if (timestamp == Long.MAX_VALUE) {
			convergedTracker.remove(context);
			return null;
		}

		Tuple2<Long, Boolean> converged = convergedTracker.get(context);
		if (converged == null || (timestamp > converged.f0 && !converged.f1)) {
			// this is the new highest watermark for this context,
			// so we put it in the convergedTracker until it comes
			// back or gets replaced by a higher one
			convergedTracker.put(context, new Tuple2(timestamp, true));

			// also we need to forward the watermark unchanged
			return watermark;
		} else if (timestamp > converged.f0 && converged.f1) {
			// a tracked watermark came back and
			// we haven't seen any event with this context for a
			// whole loop iteration, so the context is done and
			// we send out the watermark(context, MAX_LONG)
			return new Watermark(context, Long.MAX_VALUE);
		}
		return null;
	}
}
