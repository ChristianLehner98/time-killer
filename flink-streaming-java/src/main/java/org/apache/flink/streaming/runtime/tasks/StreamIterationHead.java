/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.io.BlockingQueueBroker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.progress.StreamIterationTermination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public class StreamIterationHead<OUT> extends OneInputStreamTask<OUT, OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(StreamIterationHead.class);

	private volatile boolean running = true;
//	private StreamIterationTermination termination = new StructuredIterationTermination(20);
	
	private StreamIterationTermination termination;
	// ------------------------------------------------------------------------

	

	@Override
	protected void run() throws Exception {
		termination = getConfiguration().getTerminationFunction(Thread.currentThread().getContextClassLoader());
		final String iterationId = getConfiguration().getIterationId();
		if (iterationId == null || iterationId.length() == 0) {
			throw new Exception("Missing iteration ID in the task configuration");
		}
		
		final String brokerID = createBrokerIdString(getEnvironment().getJobID(), iterationId ,
				getEnvironment().getTaskInfo().getIndexOfThisSubtask());
		
		final long iterationWaitTime = getConfiguration().getIterationWaitTime();
		final boolean shouldWait = iterationWaitTime > 0;

		final BlockingQueue<StreamElement> dataChannel = new ArrayBlockingQueue<>(100000);

		// offer the queue for the tail
		BlockingQueueBroker.INSTANCE.handIn(brokerID, dataChannel);
		LOG.info("Iteration head {} added feedback queue under {}", getName(), brokerID);

		// do the work 
		try {
			@SuppressWarnings("unchecked")
			RecordWriterOutput<OUT>[] outputs = (RecordWriterOutput<OUT>[]) getStreamOutputs();

			while (running) {
				StreamElement nextElement = shouldWait ?
					dataChannel.poll(iterationWaitTime, TimeUnit.MILLISECONDS) :
					dataChannel.take();

				if (nextElement != null) {
					if(nextElement.isWatermark()) {
						Watermark mark = nextElement.asWatermark();

						termination.observeWatermark(mark);
						if(termination.terminate(mark.getContext())) {
							mark.setIterationDone(true);
						}

						mark.forwardTimestamp();
						for (RecordWriterOutput<OUT> output : outputs) {
							System.out.println("@HEAD: " + mark);
							output.emitWatermark(mark);
						}
					} else if(nextElement.isRecord()) {
						StreamRecord record = nextElement.asRecord();
						termination.observeRecord(record);
						record.forwardTimestamp();
						for (RecordWriterOutput<OUT> output : outputs) {

							output.collect(record);
						}
					}
				}
				else {
					// done
					break;
				}
			}
		}
		finally {
			// make sure that we remove the queue from the broker, to prevent a resource leak
			BlockingQueueBroker.INSTANCE.remove(brokerID);
			LOG.info("Iteration head {} removed feedback queue under {}", getName(), brokerID);
		}
	}

	@Override
	protected void cancelTask() {
		running = false;
	}

	// ------------------------------------------------------------------------

	@Override
	public void init() {
		// does not hold any resources, no initialization necessary
	}

	@Override
	protected void cleanup() throws Exception {
		// does not hold any resources, no cleanup necessary
	}
	
	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Creates the identification string with which head and tail task find the shared blocking
	 * queue for the back channel. The identification string is unique per parallel head/tail pair
	 * per iteration per job.
	 * 
	 * @param jid The job ID.
	 * @param iterationID The id of the iteration in the job.
	 * @param subtaskIndex The parallel subtask number
	 * @return The identification string.
	 */
	public static String createBrokerIdString(JobID jid, String iterationID, int subtaskIndex) {
		return jid + "-" + iterationID + "-" + subtaskIndex;
	}

}
