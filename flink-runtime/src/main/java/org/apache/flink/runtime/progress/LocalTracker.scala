package org.apache.flink.runtime.progress

import java.lang.Long
import java.lang.Boolean
import java.util

import akka.actor.{Actor, ActorRef}
import org.apache.flink.runtime.progress.messages._
import org.apache.flink.api.java.tuple.{Tuple3 => JTuple3}
import java.util.{Collections, List => JList}

import org.apache.flink.runtime.jobgraph.JobVertexID
import org.apache.flink.runtime.messages.JobManagerMessages.CancelJob
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

class LocalTracker() extends Actor {
  private var localOperatorProgress: Map[Integer, LocalOperatorProgress] = Map()
  private var otherNodes : Set[ActorRef] = _
  private var pathSummaries: java.util.Map[Integer, java.util.Map[Integer, PartialOrderMinimumSet]] = _
  // used to buffer up progress messages until the connection to the central tracker is established and we got the path summaries
  private var initProgressBuffer: List[(ActorRef, ProgressUpdate)] = List()
  private var maxScopeLevel: Integer = 0
  private var LOG: Logger = LoggerFactory.getLogger(classOf[LocalTracker])

  private var seenInstances = Set[(JobVertexID,Integer)]()
  private var numberOfGlobalInstances: Integer = _

  def receive : Receive = {
    case progress: ProgressUpdate =>
      if (pathSummaries != null && seenInstances.size == numberOfGlobalInstances) {
        if(initProgressBuffer.nonEmpty) {
          for ((from, progress) <- initProgressBuffer.reverse) {
            update(progress, from)
          }
          initProgressBuffer = List()
        }
        update(progress, sender())
      } else {
        initProgressBuffer = (sender(), progress) :: initProgressBuffer
      }

    case init: InitLocalTracker =>
      // initialisation through central tracker giving us the pathSummaries and references to the other nodes
      pathSummaries = init.pathSummaries
      otherNodes = init.otherNodes.filter(!_.equals(self))
      maxScopeLevel = init.maxScopeLevel
      numberOfGlobalInstances = init.numberOfGlobalInstances
      if(seenInstances.size == numberOfGlobalInstances) {
        for ((from, progress) <- initProgressBuffer.reverse) {
          update(progress, from)
        }
      }
      initProgressBuffer = List()

    case hello: InstanceReady =>
      if(!otherNodes.contains(sender())) {
        // hello comes from local operator and needs to be broadcast to other nodes
        broadcastHello(hello)
      }
      seenInstances += (hello.operatorId -> hello.instanceId)

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

      broadcastDone(IsDone(notificationRequest.isDone, notificationRequest.getOperatorId, notificationRequest.getInstanceId, notificationRequest.getTimestamp))

      // add notification to pending notifications of operator
      opProgress.addNotification(
        notificationRequest.getTimestamp,
        notificationRequest.getInstanceId,
        notificationRequest.isDone,
        sender()
      )

      if(notificationRequest.isDone) LOG.info(opProgress.toString())

      // send notifications that have eventually been hold back due to missing termination
      // information from other operator instances
      for( (actorRef, notification) <- opProgress.popReadyNotifications()) {
        //LOG.info("DUE TO TERMINATION" + notification.toString + " " + actorRef)
        actorRef ! new ProgressNotification(new util.LinkedList[Long](notification.getTimestamp), notification.isDone)
      }

    case IsDone(done, operatorId, instanceId, timestamp) =>
      if(localOperatorProgress.contains(operatorId)) {
        localOperatorProgress(operatorId).otherNodeDone(done, timestamp, instanceId)
        if(done) LOG.info(localOperatorProgress(operatorId).toString())
        for( (actorRef, notification) <- localOperatorProgress(operatorId).popReadyNotifications()) {
          //System.out.println("DUE TO DONE: " + notification)
          actorRef ! new ProgressNotification(new util.LinkedList[Long](timestamp), notification.isDone)
        }
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

    // update progress
    var it = localOperatorProgress.iterator
    while(it.hasNext) {
      var (target: Integer, targetProgress: LocalOperatorProgress) = it.next()
      val progIt = progress.getEntries.asScala.iterator
      while (progIt.hasNext) {
        val (pointstamp: JTuple3[Integer,JList[Long], Boolean], delta: Integer) = progIt.next()
        val src: Integer = pointstamp.f0
        val timestamp: java.util.List[Long] = pointstamp.f1
        val isInternal: Boolean = pointstamp.f2

        for (summary: java.util.List[Long] <- getPathSummaries(src, target, isInternal)) {
          val progressAtTarget = resultsIn(timestamp, summary)
          targetProgress.updateFrontier(progressAtTarget, delta)
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
        //LOG.info("DUE TO NORMAL: " + tuple._2 + " " + tuple._1)
      }
    }
  }

  private def broadcastUpdate(message : ProgressUpdate) : Unit = {
    for(node : ActorRef <- otherNodes) {
      node ! message
    }
  }

  private def broadcastHello(message : InstanceReady) : Unit = {
    for(node : ActorRef <- otherNodes) {
      node ! message
    }
  }

  private def broadcastDone(message : IsDone) : Unit = {
    //LOG.info(message.timestamp + " " + message.operatorId + " / " + message.instanceId + "done to: " + otherNodes)
    for(node : ActorRef <- otherNodes) {
      node ! message
    }
  }

  /**
    *  
    * @param ts : the progress update received from the first operator (look below)
    * @param summary : a path between the operator where progress was reported and the operator that requests a progress notification
    * @return a timestamp (calculated forward) of the estimated minimum time at the second operator (look up) computed through the summary path
    */
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
