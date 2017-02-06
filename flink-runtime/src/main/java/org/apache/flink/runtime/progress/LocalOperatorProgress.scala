package org.apache.flink.runtime.progress

import akka.actor.ActorRef
import java.lang.Long

import org.apache.flink.runtime.progress.messages.ProgressNotification

class LocalOperatorProgress(parallelism:Integer, scopeLevel: Integer) {
  case class InstanceStatus(actorRef: ActorRef, instanceId: Integer, done: Boolean)

  private var pendingNotifications: Map[java.util.List[Long], Set[InstanceStatus]] = Map().withDefaultValue(Set())
  private val frontier: PartialOrderProgressAggregator = new PartialOrderProgressAggregator(scopeLevel)

  def addNotification(timestamp: java.util.List[Long], instanceId: Integer, done: Boolean, actorRef: ActorRef): Unit = {
    pendingNotifications += (timestamp -> (pendingNotifications(timestamp) + InstanceStatus(actorRef, instanceId, done)))
  }

  def updateFrontier(timestamp: java.util.List[Long], delta: Integer): Boolean = {
    frontier.update(timestamp, delta)
  }

  def readyNotifications(): Set[(ActorRef, ProgressNotification)] = {
    var result: Set[(ActorRef, ProgressNotification)] = Set()
    for( (timestamp: java.util.List[Long], instanceStatuses: Set[InstanceStatus]) <- pendingNotifications ) {
      if(frontier.greaterThan(timestamp)) {
        var allDone = instanceStatuses.forall(_.done)
        var allRequested = instanceStatuses.size == parallelism

        if(!allDone || allRequested) {
          for(instanceStatus <- instanceStatuses) {
            val notification = new ProgressNotification(timestamp, allDone)
            result += (instanceStatus.actorRef -> notification)
          }
        }
      }
    }
    result
  }
}
