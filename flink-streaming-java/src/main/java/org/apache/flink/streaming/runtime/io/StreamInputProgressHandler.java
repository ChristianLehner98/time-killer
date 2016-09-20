package org.apache.flink.streaming.runtime.io;


import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamInputProgressHandler {
	private int numberOfInputChannels;

	private Map<List<Long>,Long>[] watermarks;
	private Map<List<Long>,Long> lastEmittedWatermarks = new HashMap<>();

	public StreamInputProgressHandler(int numberOfInputChannels) {
		this.numberOfInputChannels = numberOfInputChannels;
		watermarks = new HashMap[numberOfInputChannels];
		for(int i=0; i<numberOfInputChannels; i++) {
			watermarks[i] = new HashMap<>();
		}
	}

	protected void adaptTimestamp(StreamElement element, int operatorLevel, boolean adaptRecords) {
		if(!element.isWatermark() && !adaptRecords) return;

		int elementLevel = getContextSize(element);
		if(elementLevel == operatorLevel) return;
		else if(elementLevel == operatorLevel-1) addTimestamp(element);
		else if(elementLevel == operatorLevel+1) removeTimestamp(element);
		else throw new IllegalStateException("Got element with wrong timestamp level");
	}

	private int getContextSize(StreamElement element) {
		if(element.isWatermark()) return element.asWatermark().getContext().size();
		return element.asRecord().getContext().size();
	}

	private void addTimestamp(StreamElement element) {
		if(element.isWatermark()) element.asWatermark().addNestedTimestamp(0);
		element.asRecord().addNestedTimestamp(0);
	}

	private void removeTimestamp(StreamElement element) {
		if(element.isWatermark()) element.asWatermark().removeNestedTimestamp();
		element.asRecord().removeNestedTimestamp();
	}

	protected Watermark getNextWatermark(Watermark watermark, int currentChannel) {
		Long timestamp = watermark.getTimestamp();
		List<Long> context = watermark.getContext();

		// Check if whole context is finished and clean up
		if(watermark.getTimestamp() == Long.MAX_VALUE) {
			watermarks[currentChannel].remove(context);
			lastEmittedWatermarks.remove(context);
			return new Watermark(watermark);
		}
		// Update local watermarks and eventually send out a new
		Long currentMax = watermarks[currentChannel].get(context);
		// Only go on if the current timestamp is actually higher for this context
		if(currentMax == null || timestamp > currentMax) {
			watermarks[currentChannel].put(context, timestamp);

			// find out the minimum over all input channels for this context
			Long newMin = Long.MAX_VALUE;
			for(int i=0; i<numberOfInputChannels; i++) {
				Long channelMax = watermarks[i].get(context);
				if(channelMax == null) return null;
				if(channelMax < newMin) newMin = channelMax;
			}

			// if the new minimum of all channels is larger than the last emitted watermark
			// put out a new one
			Long lastEmitted = lastEmittedWatermarks.get(context);
			if(lastEmitted == null || newMin > lastEmitted) {
				lastEmittedWatermarks.put(context, newMin);
				return new Watermark(context, newMin);
			}
		}
		return null;
	}

}
