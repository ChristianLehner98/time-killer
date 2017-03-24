package org.apache.flink.runtime.progress

import akka.actor.{Actor, ActorRef}
import org.apache.flink.api.common.JobID
import org.apache.flink.runtime.instance.InstanceID
import org.apache.flink.runtime.messages.JobManagerMessages.CancelJob
import org.apache.flink.runtime.progress.messages.{InitLocalTracker, NumberOfActiveTaskManagers, RegisterLocalTracker}
import org.apache.flink.api.java.tuple.Tuple2
import org.slf4j.{Logger, LoggerFactory}

class CentralTracker(pathSummaries: java.util.Map[Integer, java.util.Map[Integer, PartialOrderMinimumSet]], maxScopeLevel: Integer, numberOfGlobalInstances: Integer) extends Actor {
  private var localTrackers = Set[ActorRef]()
  private var LOG: Logger = LoggerFactory.getLogger(classOf[CentralTracker])
  private var taskManagerCount: Option[Int] = None
  private var hasBeenSentOut: Boolean = false

  def receive() : Receive = {
    case register @ RegisterLocalTracker(jobID: JobID, taskManagerId: InstanceID, actorRef: ActorRef) =>
      LOG.info(register.toString)
      localTrackers += actorRef
      checkForCompleteness()

    case NumberOfActiveTaskManagers(taskManagers: Int) =>
      taskManagerCount = Some(taskManagers)
      checkForCompleteness()

    case CancelJob =>
      for(actorRef <- localTrackers) {
        actorRef ! CancelJob
      }
  }

  def checkForCompleteness() : Unit = {
    if(!hasBeenSentOut && taskManagerCount.isDefined && localTrackers.size == taskManagerCount.get) {
      // now all task managers registered and we can send out the initialisation
      hasBeenSentOut = true
      for(actorRef <- localTrackers) {
        actorRef ! InitLocalTracker(localTrackers, pathSummaries, maxScopeLevel, numberOfGlobalInstances)
      }
    }
  }
}
