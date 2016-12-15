package org.apache.flink.streaming.runtime.io;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

import java.io.Serializable;
import java.util.*;

public class StreamInputProgressHandler implements Serializable {
	private int numberOfInputChannels;

	private Map<List<Long>,Tuple2<Long,Boolean>>[] watermarks;
	private Map<List<Long>,Long> lastEmittedWatermarks = new HashMap<>();

	public StreamInputProgressHandler(int numberOfInputChannels) {
		this.numberOfInputChannels = numberOfInputChannels;
		watermarks = new HashMap[numberOfInputChannels];
		for(int i=0; i<numberOfInputChannels; i++) {
			watermarks[i] = new HashMap<>();
		}
	}

	public StreamElement adaptTimestamp(StreamElement element, int operatorLevel) {
		if(element.isLatencyMarker()) return element;
		int elementLevel = getContextSize(element);
		if(elementLevel == operatorLevel) return element;
		else if(elementLevel == operatorLevel-1) addTimestamp(element);
		else if(elementLevel == operatorLevel+1) {
			if(element.isWatermark() && element.asWatermark().getTimestamp() != Long.MAX_VALUE) {
				// this is a watermark coming out of an iteration which is not done yet!
				return null;
			} else {
				removeTimestamp(element);
			}
		}
		else throw new IllegalStateException("Got element with wrong timestamp level");
		return element;
	}

	private int getContextSize(StreamElement element) {
		if(element.isWatermark()) return element.asWatermark().getContext().size();
		return element.asRecord().getContext().size();
	}

	private void addTimestamp(StreamElement element) {
		if(element.isWatermark()) element.asWatermark().addNestedTimestamp(0);
		else element.asRecord().addNestedTimestamp(0);
	}

	private void removeTimestamp(StreamElement element) {
		if(element.isWatermark()) element.asWatermark().removeNestedTimestamp();
		else element.asRecord().removeNestedTimestamp();
	}

	public Watermark getNextWatermark(Watermark watermark, int currentChannel) {
		Long timestamp = watermark.getTimestamp();
		List<Long> context = watermark.getContext();

		// Check if whole context is finished and clean up
		if(watermark.getTimestamp() == Long.MAX_VALUE) {
			watermarks[currentChannel].remove(context);
			lastEmittedWatermarks.remove(context);
			return null;
		}
		// Update local watermarks and eventually send out a new
		Long currentMax = watermarks[currentChannel].get(context) != null ?
			watermarks[currentChannel].get(context).f0 : null;
		// Only go on if the current timestamp is actually higher for this context
		if(currentMax == null || (timestamp != null && timestamp > currentMax)) {
			watermarks[currentChannel].put(context, new Tuple2(timestamp,watermark.iterationDone()));

			// find out the minimum over all input channels for this context
			Long newMin = Long.MAX_VALUE;
			boolean isDone = true;
			for(int i=0; i<numberOfInputChannels; i++) {
				Long channelMax = watermarks[i].get(context).f0;
				if(channelMax == null) return null;

				if(!watermarks[i].get(context).f1) isDone = false;
				if(channelMax < newMin) newMin = channelMax;
			}

			if(isDone) {
				return new Watermark(context, Long.MAX_VALUE, true, watermark.iterationOnly());
			} else {
				// if the new minimum of all channels is larger than the last emitted watermark
				// put out a new one
				Long lastEmitted = lastEmittedWatermarks.get(context);
				if (lastEmitted == null || newMin > lastEmitted) {
					lastEmittedWatermarks.put(context, newMin);
					return new Watermark(context, newMin, false, watermark.iterationOnly());
				}
			}
		}
		return null;
	}

}
