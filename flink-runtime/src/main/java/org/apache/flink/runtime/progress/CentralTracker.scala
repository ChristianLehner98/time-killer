package org.apache.flink.runtime.progress

import akka.actor.{Actor, ActorRef}
import org.apache.flink.api.common.JobID
import org.apache.flink.runtime.instance.InstanceID
import org.apache.flink.runtime.messages.JobManagerMessages.CancelJob
import org.apache.flink.runtime.progress.messages.{InitLocalTracker, RegisterLocalTracker}
import org.apache.flink.api.java.tuple.Tuple2

class CentralTracker(taskManagerCount: Integer, pathSummaries: java.util.Map[Integer, java.util.Map[Integer, Tuple2[PartialOrderMinimumSet,Integer]]]) extends Actor {
  private var localTrackers = Set[ActorRef]()

  def receive() : Receive = {
    case RegisterLocalTracker(jobID: JobID, taskManagerId: InstanceID, actorRef: ActorRef) =>
      val countBefore = localTrackers.size
      localTrackers += actorRef

      if(countBefore < taskManagerCount && localTrackers.size == taskManagerCount) {
        // now all task managers registered and we can send out the initialisation
        for(actorRef <- localTrackers) {
          actorRef ! InitLocalTracker(localTrackers, pathSummaries)
        }
      }

    case CancelJob =>
      for(actorRef <- localTrackers) {
        actorRef ! CancelJob
      }
  }
}
