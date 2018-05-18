/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

import java.io.Serializable;

public class ProgressTrackingUtils implements Serializable {
	
	
	public static StreamElement adaptTimestamp(StreamElement element, int operatorLevel) {
		if (element.isLatencyMarker()) {
			return element;
		}
		int elementLevel = getContextSize(element);
		if (elementLevel == operatorLevel) {
			return element;
		} else if (elementLevel == operatorLevel - 1) {
			addTimestamp(element);
		} else if (elementLevel == operatorLevel + 1) {
			if (element.isWatermark() && element.asWatermark().getTimestamp() != Long.MAX_VALUE) {
				// this is a watermark coming out of an iteration which is not done yet!
				return null;
			} else {
				removeTimestamp(element);
			}
		} else {
			throw new IllegalStateException("Got element with wrong timestamp level");
		}
		return element;
	}

	static int getContextSize(StreamElement element) {
		if (element.isWatermark()) {
			return element.asWatermark().getContext().size();
		}
		return element.asRecord().getContext().size();
	}

	static void addTimestamp(StreamElement element) {
		if (element.isWatermark()) {
			element.asWatermark().addNestedTimestamp(0);
		} else {
			element.asRecord().addNestedTimestamp(0);
		}
	}

	static void removeTimestamp(StreamElement element) {
		if (element.isWatermark()) {
			element.asWatermark().removeNestedTimestamp();
		} else {
			element.asRecord().removeNestedTimestamp();
		}
	}
}
