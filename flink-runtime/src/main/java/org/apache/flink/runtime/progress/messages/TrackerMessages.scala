package org.apache.flink.runtime.progress.messages

import akka.actor.ActorRef
import org.apache.flink.api.common.JobID
import org.apache.flink.runtime.instance.InstanceID
import org.apache.flink.runtime.progress.PartialOrderMinimumSet
import java.lang.Long
import java.util.List

import org.apache.flink.runtime.jobgraph.JobVertexID;

case class RegisterLocalTracker(jobId: JobID, taskManagerId: InstanceID, actorRef: ActorRef)
case class InitLocalTracker(otherNodes: Set[ActorRef], pathSummaries: java.util.Map[Integer, java.util.Map[Integer, PartialOrderMinimumSet]],
                            maxScopeLevel: Integer, numberOfGlobalInstances: Integer, interval: Integer)
case class NumberOfActiveTaskManagers(value: Int)
case class IsDone(done: Boolean, operatorId: Integer, instanceId: Integer, timestamp: java.util.List[java.lang.Long])
case class ProgressMetricsReport(jobId: JobID, operatorId: Integer, instanceId: Integer, context: List[Long], step: Long, startTS: Long, localEndTS: Long, endTS: Long)
case class InstanceReady(operatorId: JobVertexID, instanceId: Integer)
