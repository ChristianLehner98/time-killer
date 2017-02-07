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
  private var localOperatorProgress: Map[Integer, LocalOperatorProgress] = Map()
  private var otherNodes : Set[ActorRef] = _
  private var pathSummaries: java.util.Map[Integer, java.util.Map[Integer, Tuple2[PartialOrderMinimumSet,Integer]]] = _
  // used to buffer up progress messages until the connection to the central tracker is established and we got the path summaries
  private var initProgressBuffer: List[(ActorRef, ProgressUpdate)] = List()

  def receive : Receive = {
    case progress: ProgressUpdate =>
      print(progress)
      if (pathSummaries != null) {
        update(progress, sender())
      } else {
        initProgressBuffer = (sender(), progress) :: initProgressBuffer
      }

    case init: InitLocalTracker =>
      print(init)
      // initialisation through central tracker giving us the pathSummaries and references to the other nodes
      pathSummaries = init.pathSummaries
      otherNodes = init.otherNodes
      for((from, progress) <- initProgressBuffer.reverse) {
        update(progress, from)
      }
      initProgressBuffer = List()

    case registration: ProgressRegistration =>
      print(registration)
      if(localOperatorProgress.get(registration.getOperatorId).isEmpty) {
        localOperatorProgress +=
          (registration.getOperatorId ->
            new LocalOperatorProgress(registration.getParallelism, registration.getScopeLevel))
      }

    case notificationRequest: ProgressNotificationRequest =>
      print(notificationRequest)
      val opProgress = localOperatorProgress(notificationRequest.getOperatorId)

      // add notification to pending notifications of operator
      opProgress.addNotification(
        notificationRequest.getTimestamp,
        notificationRequest.getInstanceId,
        notificationRequest.isDone,
        sender()
      )

      // send notifications that have eventually been hold back due to missing termination
      // information from other operator instances
      for( (actorRef, notification) <- opProgress.readyNotifications()) {
        actorRef ! notification
      }

    case CancelJob =>
      print("cancel")
      context.stop(self)
  }

  private def update(progress : ProgressUpdate, from: ActorRef): Unit = {
    if(!otherNodes.contains(from)) {
      // update comes from local operator and needs to be broadcast to other nodes
      broadcastUpdate(progress)
    }

    for((op: Integer, opProgress: LocalOperatorProgress) <- localOperatorProgress) {
      // update progress
      for( (pointstamp : JTuple2[Integer, JList[Long]], delta) <- progress.getEntries.asScala) {
        val from : Integer = pointstamp.f0
        val timestamp : java.util.List[Long] = pointstamp.f1
        for (summary: java.util.List[Long] <- getPathSummaries(from, op)) {
          val timeAtTo = resultsIn(timestamp, summary)
          opProgress.updateFrontier(timeAtTo, delta)
        }
      }

      // send notifications if any new ready
      for( (actorRef, notification) <- opProgress.readyNotifications()) {
        actorRef ! notification
      }
    }
  }

  private def broadcastUpdate(progress : ProgressUpdate) : Unit = {
    for(node : ActorRef <- otherNodes) {
      node ! progress
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
}
