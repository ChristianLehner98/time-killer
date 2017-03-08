package org.apache.flink.runtime.progress

import akka.actor.ActorRef
import java.util.{List => JList}
import java.lang.Long
import java.util

import org.apache.flink.runtime.progress.messages.ProgressNotification
import org.slf4j.{Logger, LoggerFactory}

class LocalOperatorProgress(parallelism:Integer, maxScopeLevel: Integer) {
  case class Instance(actorRef: ActorRef, instanceId: Integer)

  private var pendingNotifications: Map[java.util.List[Long], Set[Instance]] = Map().withDefaultValue(Set())
  private val frontier: PartialOrderProgressAggregator = new PartialOrderProgressAggregator(maxScopeLevel+1)
  private var doneCollector: Map[java.util.List[Long], Set[(Integer,Boolean)]] = Map().withDefaultValue(Set())

  private var LOG: Logger = LoggerFactory.getLogger(classOf[LocalOperatorProgress])

  def addNotification(timestamp: java.util.List[Long], instanceId: Integer, done: Boolean, actorRef: ActorRef): Unit = {
    var timestampCopy: java.util.List[Long] = new util.LinkedList[Long](timestamp)
    System.out.println("NOTIFICATION REQUEST: " + instanceId + " -> " + timestamp)
    System.out.println("PENDING BEFORE: " + pendingNotifications)
    pendingNotifications += (timestampCopy -> (pendingNotifications(timestampCopy) + Instance(actorRef, instanceId)))
    doneCollector += (timestamp -> (doneCollector(timestamp) + (instanceId -> done)))
    System.out.println("PENDING AFTER: " + pendingNotifications)
  }

  def updateFrontier(timestamp: java.util.List[Long], delta: Integer): Boolean = {
    frontier.update(timestamp, delta)
  }

  def otherNodeDone(done: Boolean, timestamp: java.util.List[Long], instanceId: Integer): Unit = {
    doneCollector += (timestamp -> (doneCollector(timestamp) + (instanceId -> done)))
  }

  // TODO free resources when popping out notifications
  def popReadyNotifications(): Set[(ActorRef, ProgressNotification)] = {
    var result: Set[(ActorRef, ProgressNotification)] = Set()

    pendingNotifications = pendingNotifications.filter {
      case (timestamp: JList[Long], instances: Set[Instance]) =>
        var allInstancesDone = doneCollector(timestamp).forall(_._2)
        var allDoneInfoReceived = doneCollector(timestamp).size == parallelism

        System.out.println(doneCollector(timestamp))
        System.out.println("ALLDONE: " + allInstancesDone + ", " + "ALLINFO: " + allDoneInfoReceived)

        if(frontier.ready(timestamp) && (!allInstancesDone || allDoneInfoReceived)) {
          for(instance <- instances) {
            val notification = new ProgressNotification(timestamp, allInstancesDone)
            result += (instance.actorRef -> notification)
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
