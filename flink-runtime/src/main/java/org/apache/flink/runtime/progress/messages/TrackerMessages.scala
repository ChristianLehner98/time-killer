package org.apache.flink.runtime.progress.messages

import akka.actor.ActorRef
import org.apache.flink.api.common.JobID
import org.apache.flink.runtime.instance.InstanceID
import org.apache.flink.runtime.progress.PartialOrderMinimumSet
import org.apache.flink.api.java.tuple.Tuple2

case class RegisterLocalTracker(jobId: JobID, taskManagerId: InstanceID, actorRef: ActorRef)
case class InitLocalTracker(otherNodes: Set[ActorRef], pathSummaries: java.util.Map[Integer, java.util.Map[Integer, PartialOrderMinimumSet]], maxScopeLevel: Integer)
case class NumberOfActiveTaskManagers(value: Int)
