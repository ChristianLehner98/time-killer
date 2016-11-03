package org.apache.flink.streaming.runtime.io;


import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

import java.util.*;

public class StreamInputProgressHandler {
	private int numberOfInputChannels;

	private SortedSet<Watermark> watermarkBuffer;

	private Map<List<Long>,Long>[] watermarks;
	private Map<List<Long>,Long> lastEmittedWatermarks = new HashMap<>();

	public StreamInputProgressHandler(int numberOfInputChannels) {
		this.numberOfInputChannels = numberOfInputChannels;
		watermarks = new HashMap[numberOfInputChannels];
		for(int i=0; i<numberOfInputChannels; i++) {
			watermarks[i] = new HashMap<>();
		}
	}

	protected StreamElement adaptTimestamp(StreamElement element, int operatorLevel, boolean adaptRecords) {
		if(!element.isWatermark() && !adaptRecords) return element;

		int elementLevel = getContextSize(element);
		if(elementLevel == operatorLevel) return element;
		else if(elementLevel == operatorLevel-1) addTimestamp(element, adaptRecords);
		else if(elementLevel == operatorLevel+1) {
			if(element.isWatermark() && element.asWatermark().getTimestamp() == Long.MAX_VALUE) {
				Watermark watermark = element.asWatermark();
			} else {
				removeTimestamp(element, adaptRecords);
			}
		}
		else throw new IllegalStateException("Got element with wrong timestamp level");
		return element;
	}

	private int getContextSize(StreamElement element) {
		if(element.isWatermark()) return element.asWatermark().getContext().size();
		return element.asRecord().getContext().size();
	}

	private void addTimestamp(StreamElement element, boolean adaptRecords) {
		if(element.isWatermark()) element.asWatermark().addNestedTimestamp(0);
		else if(adaptRecords) element.asRecord().addNestedTimestamp(0);
	}

	private void removeTimestamp(StreamElement element, boolean adaptRecords) {
		if(element.isWatermark()) element.asWatermark().removeNestedTimestamp();
		else if(adaptRecords) element.asRecord().removeNestedTimestamp();
	}

	protected Collection<Watermark> getNextWatermark(Watermark watermark, int currentChannel) {
		Set<Watermark> result = new HashSet<>(watermarkBuffer.headSet(watermark));

		Long timestamp = watermark.getTimestamp();
		List<Long> context = watermark.getContext();

		// Check if whole context is finished and clean up
		if(watermark.getTimestamp() == Long.MAX_VALUE) {
			watermarks[currentChannel].remove(context);
			lastEmittedWatermarks.remove(context);
			result.add(new Watermark(watermark));
			return result;
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
