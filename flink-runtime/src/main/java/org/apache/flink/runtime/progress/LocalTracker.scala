package org.apache.flink.runtime.progress

import java.lang.Long
import java.lang.Boolean
import java.util

import akka.actor.{Actor, ActorRef}
import org.apache.flink.runtime.progress.messages._
import org.apache.flink.api.java.tuple.{Tuple3 => JTuple3}
import java.util.{Collections, List => JList}

import org.apache.flink.runtime.messages.JobManagerMessages.CancelJob

import scala.collection.JavaConverters._

class LocalTracker() extends Actor {
  private var localOperatorProgress: Map[Integer, LocalOperatorProgress] = Map()
  private var otherNodes : Set[ActorRef] = _
  private var pathSummaries: java.util.Map[Integer, java.util.Map[Integer, PartialOrderMinimumSet]] = _
  // used to buffer up progress messages until the connection to the central tracker is established and we got the path summaries
  private var initProgressBuffer: List[(ActorRef, ProgressUpdate)] = List()
  private var maxScopeLevel: Integer = 0

  def receive : Receive = {
    case progress: ProgressUpdate =>
      //System.out.println(progress)
      if (pathSummaries != null) {
        update(progress, sender())
        //System.out.println(localOperatorProgress(7))
      } else {
        initProgressBuffer = (sender(), progress) :: initProgressBuffer
      }

    case init: InitLocalTracker =>
      // initialisation through central tracker giving us the pathSummaries and references to the other nodes
      pathSummaries = init.pathSummaries
      otherNodes = init.otherNodes
      maxScopeLevel = init.maxScopeLevel
      for((from, progress) <- initProgressBuffer.reverse) {
        update(progress, from)
      }
      initProgressBuffer = List()

    case registration: ProgressRegistration =>
      //System.out.println(registration)
      if(localOperatorProgress.get(registration.getOperatorId).isEmpty) {
        localOperatorProgress +=
          (registration.getOperatorId ->
            new LocalOperatorProgress(registration.getParallelism, maxScopeLevel))
      }

    case notificationRequest: ProgressNotificationRequest =>
      //System.out.println(notificationRequest)
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
      for( (actorRef, notification) <- opProgress.popReadyNotifications()) {
        //System.out.println(notification)
        actorRef ! new ProgressNotification(new util.LinkedList[Long](notification.getTimestamp), notification.isDone)
      }

    case CancelJob =>
      //print("cancel")
      context.stop(self)
  }

  private def update(progress : ProgressUpdate, from: ActorRef): Unit = {
    if(!otherNodes.contains(from)) {
      // update comes from local operator and needs to be broadcast to other nodes
      broadcastUpdate(progress)
    }

    var it = localOperatorProgress.iterator
    while(it.hasNext) {
      var (op: Integer, opProgress: LocalOperatorProgress) = it.next()
      // update progress
      val progIt = progress.getEntries.asScala.iterator
      while (progIt.hasNext) {
        val (pointstamp: JTuple3[Integer,JList[Long], Boolean], delta: Integer) = progIt.next()
        val from: Integer = pointstamp.f0
        val timestamp: java.util.List[Long] = pointstamp.f1
        val isInternal: Boolean = pointstamp.f2

        for (summary: java.util.List[Long] <- getPathSummaries(from, op, isInternal)) {
          val timeAtTo = resultsIn(timestamp, summary)
          opProgress.updateFrontier(timeAtTo, delta)
        }
      }
    }

    // send notifications if any new ready
    it = localOperatorProgress.iterator
    while(it.hasNext) {
      var (_: Integer, opProgress: LocalOperatorProgress) = it.next()
      val result: Set[(ActorRef, ProgressNotification)] = opProgress.popReadyNotifications()

      val it2: Iterator[(ActorRef, ProgressNotification)] = result.iterator
      while(it2.hasNext) {
        val tuple: (ActorRef, ProgressNotification) = it2.next()
        tuple._1 ! new ProgressNotification(new util.LinkedList[Long](tuple._2.getTimestamp), tuple._2.isDone)
      }
    }
  }

  private def broadcastUpdate(progress : ProgressUpdate) : Unit = {
    for(node : ActorRef <- otherNodes) {
      node ! progress
    }
  }

  private def resultsIn(ts: java.util.List[Long], summary: java.util.List[Long]): java.util.List[Long] = {
    val paddedTs: java.util.List[Long] = new util.LinkedList[Long]()
    for(i <- 0 to maxScopeLevel) {
      if(i<ts.size()) paddedTs.add(ts.get(i))
      else paddedTs.add(0L)
    }

    var result : java.util.List[Long] = new java.util.LinkedList[Long]()
    for((summaryPart, i) <- summary.asScala.view.zipWithIndex) {
        result.add(paddedTs.get(i) + summaryPart)
    }
    result
  }

  private def getPathSummaries(from : Integer, to: Integer, isInternal: Boolean) : Set[java.util.List[Long]] = {
    if(from == to && !isInternal) {
      return Set(new util.LinkedList(Collections.nCopies(maxScopeLevel+1, 0L)))
    }

    pathSummaries.asScala.get(from) match {
      case Some(toSummaries: java.util.Map[Integer,PartialOrderMinimumSet]) =>
        toSummaries.asScala.get(to) match {
          case Some(res) =>
            res.getElements.asScala.toSet
          case None => Set()
        }
      case None => Set()
    }
  }
}
