package org.apache.flink.runtime.progress

import java.lang.Long
import java.lang.Boolean
import java.util
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, Cancellable}
import org.apache.flink.runtime.progress.messages._
import org.apache.flink.api.java.tuple.{Tuple3 => JTuple3}
import java.util.{Collections, List => JList}

import org.apache.flink.runtime.jobgraph.JobVertexID
import org.apache.flink.runtime.messages.JobManagerMessages.CancelJob
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.collection.JavaConverters._

class LocalTracker() extends Actor {
  private var localOperatorProgress: Map[Integer, LocalOperatorProgress] = Map()
  private var otherNodes : Set[ActorRef] = _
  private var pathSummaries: java.util.Map[Integer, java.util.Map[Integer, PartialOrderMinimumSet]] = _
  // used to buffer up progress messages until the connection to the central tracker is established and we got the path summaries
  //private val initialProgress = new ProgressUpdate()
  //private var initProgressBuffer: List[(ActorRef, ProgressUpdate)] = List()
  private var maxScopeLevel: Integer = 0
  private var LOG: Logger = LoggerFactory.getLogger(classOf[LocalTracker])

  private var seenInstances = Set[(JobVertexID,Integer)]()
  private var numberOfGlobalInstances: Integer = _

  // we're now aggregating messages for a certain time interval before sending to the other nodes
  private var batchedProgress = new ProgressUpdate()
  private var interval = 200
  private var scheduler : Cancellable = _

  def receive : Receive = {
    case progress: ProgressUpdate =>
      if(!otherNodes.contains(sender())) {
        batchedProgress.mergeIn(progress)
        initScheduler()
      } else {
        update(progress)
      }

    case init: InitLocalTracker =>
      // initialisation through central tracker giving us the pathSummaries and references to the other nodes
      pathSummaries = init.pathSummaries
      otherNodes = init.otherNodes.filter(!_.equals(self))
      maxScopeLevel = init.maxScopeLevel
      numberOfGlobalInstances = init.numberOfGlobalInstances
      interval = init.interval
      initScheduler()

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

      // send notifications that have eventually been hold back due to missing termination
      // information from other operator instances
      for( (actorRef, notification) <- opProgress.popReadyNotifications()) {
        //println("DUE TO TERMINATION" + notification.toString + " " + actorRef)
        //println(localOperatorProgress)
        actorRef ! new ProgressNotification(new util.LinkedList[Long](notification.getTimestamp), notification.isDone)
      }

    case IsDone(done, operatorId, instanceId, timestamp) =>
      if(localOperatorProgress.contains(operatorId)) {
        localOperatorProgress(operatorId).otherNodeDone(done, timestamp, instanceId)
        for( (actorRef, notification) <- localOperatorProgress(operatorId).popReadyNotifications()) {
          //println("DUE TO DONE: " + notification)
          //println(localOperatorProgress)
          actorRef ! new ProgressNotification(new util.LinkedList[Long](timestamp), notification.isDone)
        }
      }

    case "broadcastBuffer" =>
      if(!batchedProgress.isEmpty) {
        update(batchedProgress)
        broadcastUpdate(batchedProgress)
        batchedProgress = new ProgressUpdate()
      }

    case CancelJob =>
      //print("cancel")
      context.stop(self)
  }

  override def postStop {
    println(localOperatorProgress)
  }

  private def initScheduler(): Unit = {
    if(scheduler == null && pathSummaries != null && seenInstances.size == numberOfGlobalInstances) {
      scheduler = context.system.scheduler.schedule(Duration.create(0, TimeUnit.MILLISECONDS), Duration.create(interval, TimeUnit.MILLISECONDS), self, "broadcastBuffer")
    }
  }

  private def update(progress : ProgressUpdate): Unit = {
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
        //println("DUE TO NORMAL: " + tuple._2 + " " + tuple._1)
        //println(localOperatorProgress)
      }
    }
  }

  private def broadcastUpdate(message : ProgressUpdate) : Unit = {
    val newProgress = new ProgressUpdate(message)
    for(node : ActorRef <- otherNodes) {
      node ! newProgress
    }
  }

  private def broadcastHello(message : InstanceReady) : Unit = {
    for(node : ActorRef <- otherNodes) {
      node ! message
    }
  }

  private def broadcastDone(message : IsDone) : Unit = {
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
