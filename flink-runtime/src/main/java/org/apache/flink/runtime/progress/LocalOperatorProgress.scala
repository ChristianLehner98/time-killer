package org.apache.flink.runtime.progress

import akka.actor.ActorRef
import java.util.{List => JList}
import java.lang.Long
import java.util

import org.apache.flink.runtime.progress.messages.ProgressNotification

class LocalOperatorProgress(parallelism:Integer, maxScopeLevel: Integer) {
  case class InstanceStatus(actorRef: ActorRef, instanceId: Integer, done: Boolean)

  private var pendingNotifications: Map[java.util.List[Long], Set[InstanceStatus]] = Map().withDefaultValue(Set())
  private val frontier: PartialOrderProgressAggregator = new PartialOrderProgressAggregator(maxScopeLevel+1)

  def addNotification(timestamp: java.util.List[Long], instanceId: Integer, done: Boolean, actorRef: ActorRef): Unit = {
    var timestampCopy: java.util.List[Long] = new util.LinkedList[Long](timestamp)
    pendingNotifications += (timestampCopy -> (pendingNotifications(timestampCopy) + InstanceStatus(actorRef, instanceId, done)))
  }

  def updateFrontier(timestamp: java.util.List[Long], delta: Integer): Boolean = {
    frontier.update(timestamp, delta)
  }

  def popReadyNotifications(): Set[(ActorRef, ProgressNotification)] = {
    var result: Set[(ActorRef, ProgressNotification)] = Set()

    pendingNotifications = pendingNotifications.filter {
      case (timestamp: JList[Long], instanceStatuses: Set[InstanceStatus]) =>
        var allDone = instanceStatuses.forall(_.done)
        var allRequested = instanceStatuses.size == parallelism

        if(frontier.ready(timestamp) && (!allDone || allRequested)) {
          for(instanceStatus <- instanceStatuses) {
            val notification = new ProgressNotification(timestamp, allDone)
            result += (instanceStatus.actorRef -> notification)
          }
          false // not pending anymore
        } else {
          true // retain as pending
        }
    }

    result
  }

  override def toString(): String = {
    /*"pending: " + pendingNotifications.keys + "\n" + "frontier: " +*/ frontier.toString
  }
}
