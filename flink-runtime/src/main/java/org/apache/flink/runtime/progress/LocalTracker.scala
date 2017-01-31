package org.apache.flink.runtime.progress

import java.lang.Long

import akka.actor.{Actor, ActorRef}
import org.apache.flink.runtime.progress.messages._
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import java.util.{List => JList}
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.runtime.messages.JobManagerMessages.CancelJob

import scala.collection.JavaConverters._

class LocalTracker() extends Actor {
  case class LocalNotificationOperator(actorRef: ActorRef, pendingNotifications: java.util.LinkedList[java.util.List[Long]], frontier: PartialOrderProgressAggregator)

  private var localOperatorProgress: Map[Integer, LocalNotificationOperator] = Map()
  private var otherNodes : Set[ActorRef] = _
  private var pathSummaries: java.util.Map[Integer, java.util.Map[Integer, Tuple2[PartialOrderMinimumSet,Integer]]] = _
  // used to buffer up progress messages until the connection to the central tracker is established and we got the path summaries
  private var initProgressBuffer: List[CountMap] = _

  def receive : Receive = {
    case progress: CountMap =>
      if (pathSummaries != null) {
        update(progress)
      } else {
        initProgressBuffer = progress :: initProgressBuffer
      }

    case init: InitLocalTracker =>
      // initialisation through central tracker giving us the pathSummaries and references to the other nodes
      pathSummaries = init.pathSummaries
      otherNodes = init.otherNodes
      for(progress <- initProgressBuffer.reverse) {
        update(progress)
      }
      initProgressBuffer = List()

    case registration: OperatorRegistration =>
      localOperatorProgress += (registration.getOperatorId ->
        LocalNotificationOperator(
          context.sender(),
          new java.util.LinkedList[java.util.List[Long]](),
          new PartialOrderProgressAggregator(registration.getScopeLevel())))

    case notificationRequest: ProgressNotificationRequest =>
      var operatorProgress = localOperatorProgress(notificationRequest.getOperatorId)
      if(operatorProgress.frontier.greaterThan(notificationRequest.getTimestamp)) {
        sendNotification(notificationRequest.getOperatorId, notificationRequest.getTimestamp)
      } else {
        operatorProgress.pendingNotifications.add(notificationRequest.getTimestamp)
      }

    case CancelJob =>
      context.stop(self)
  }

  private def update(progress : CountMap): Unit = {
    if(!otherNodes.contains(sender())) {
      // update comes from local operator and needs to be broadcast to other nodes
      broadcastUpdate(progress)
    }
    updateOperatorFrontiers(progress)
    sendAccomplishedNotifications()
  }

  private def broadcastUpdate(progress : CountMap) : Unit = {
    for(node : ActorRef <- otherNodes) {
      node ! progress
    }
  }

  private def updateOperatorFrontiers(update : CountMap) : Unit = {
    for( (pointstamp : JTuple2[Integer, JList[Long]], delta) <- update.getEntries.asScala) {
      val from : Integer = pointstamp.f0
      val timestamp : java.util.List[Long] = pointstamp.f1
      for(to : Integer <- localOperatorProgress.keys)
        for(summary : java.util.List[Long] <- getPathSummaries(from, to)) {
          val timeAtTo = resultsIn(timestamp, summary)
          localOperatorProgress.get(to) match {
            case Some(toProgress) => toProgress.frontier.update(timeAtTo, delta)
          }
        }
    }
  }

  private def resultsIn(ts: java.util.List[Long], summary: java.util.List[Long]): java.util.List[Long] = {
    var result : java.util.List[Long] = new java.util.LinkedList[Long]()
    for((summaryPart, i) <- summary.asScala.view.zipWithIndex) {
      result.add(ts.get(i) + summaryPart)
    }
    result
  }

  private def getPathSummaries(from : Integer, to: Integer) : Set[java.util.List[Long]] = {
    pathSummaries.asScala.get(to) match {
      case Some(toSummaries: java.util.Map[Integer,Tuple2[PartialOrderMinimumSet,Integer]]) =>
        toSummaries.asScala.get(from) match {
          case Some(res) => res.f0.getElements.asScala.toSet
          case None => Set()
        }
      case None => Set()
    }
  }

  private def sendNotification(operatorId: Integer, timestamp: java.util.List[Long]) = {
    localOperatorProgress(operatorId).actorRef ! new ProgressNotification(timestamp)
  }

  private def sendAccomplishedNotifications() = {
    for((id, operatorprogress) <- localOperatorProgress) {
      for(notification : java.util.List[Long] <- operatorprogress.pendingNotifications.asScala) {
        if(operatorprogress.frontier.greaterThan(notification)) {
          sendNotification(id, notification)
        }
      }
    }
  }

}
