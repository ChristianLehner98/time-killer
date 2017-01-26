package org.apache.flink.runtime.progress

import java.lang.Long
import java.util

import akka.actor.{Actor, ActorRef}
import org.apache.flink.runtime.progress.messages._

import scala.collection.JavaConverters._

class LocalTracker() extends Actor {
  case class LocalNotificationOperator(actorRef: ActorRef, pendingNotifications: util.LinkedList[java.util.List[java.lang.Long]], frontier: PartialOrderProgressAggregator)

  private var localOperatorProgress: Map[Integer, LocalNotificationOperator] = Map()
  private var otherNodes : Set[ActorRef] = _
  private var pathSummaries: java.util.Map[Integer, java.util.Map[Integer, PartialOrderMinimumSet]] = _
  // used to buffer up progress messages until the connection to the central tracker is established and we got the path summaries
  private var initProgressBuffer: List[ProgressUpdate] = _

  def receive : Unit = {
    case progress: ProgressUpdate =>
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
          new util.LinkedList[java.util.List[java.lang.Long]](),
          new PartialOrderProgressAggregator(registration.getScopeLevel())))

    case notificationRequest: ProgressNotificationRequest =>
      var operatorProgress = localOperatorProgress(notificationRequest.getOperatorId)
      if(operatorProgress.frontier.greaterThan(notificationRequest.getTimestamp)) {
        sendNotification(notificationRequest.getOperatorId, notificationRequest.getTimestamp)
      } else {
        operatorProgress.pendingNotifications.add(notificationRequest.getTimestamp)
      }
  }

  private def update(progress : ProgressUpdate): Unit = {
    if(!otherNodes.contains(sender())) {
      // update comes from local operator and needs to be broadcast to other nodes
      broadcastUpdate(progress)
    }
    updateOperatorFrontiers(progress)
    sendAccomplishedNotifications()
  }

  private def broadcastUpdate(progress : ProgressUpdate) : Unit = {
    for(node : ActorRef <- otherNodes) {
      node ! progress
    }
  }

  private def updateOperatorFrontiers(update : ProgressUpdate) : Unit = {
    for( (Tuple2(from: Integer, timestamp: java.util.List[java.lang.Long]), delta) <- update.getEntries) {
      for(to : Integer <- localOperatorProgress.keys)
        for(summary <- getPathSummaries(from, to)) {
          val timeAtTo = resultsIn(timestamp, summary)
          localOperatorProgress.get(to) match {
            case Some(toProgress) => toProgress.frontier.update(timeAtTo, delta)
          }
        }
    }
  }

  private def resultsIn(ts: java.util.List[java.lang.Long], summary: List[Long]): java.util.List[java.lang.Long] = {
    var result : java.util.List[java.lang.Long] = new util.LinkedList[Long]()
    for((summaryPart, i) <- summary.view.zipWithIndex) {
      result.add(ts.get(i) + summaryPart)
    }
    result
  }

  private def getPathSummaries(from : Integer, to: Integer) : Set[List[Long]] = {
    pathSummaries.asScala.get(to) match {
      case Some(toSummaries: java.util.Map[Integer,PartialOrderMinimumSet]) =>
        toSummaries.asScala.get(from) match {
          case Some(res) => res.getElements.toArray().toSet[List[Long]]
          case None => Set()
        }
      case None => Set()
    }
  }

  private def sendNotification(operatorId: Integer, timestamp: java.util.List[java.lang.Long]) = {
    localOperatorProgress(operatorId).actorRef ! new ProgressNotification(timestamp)
  }

  private def sendAccomplishedNotifications() = {
    for((id, operatorprogress) <- localOperatorProgress) {
      for(notification <- operatorprogress.pendingNotifications) {
        if(operatorprogress.frontier.greaterThan(notification)) {
          sendNotification(id, notification)
        }
      }
    }
  }

}
