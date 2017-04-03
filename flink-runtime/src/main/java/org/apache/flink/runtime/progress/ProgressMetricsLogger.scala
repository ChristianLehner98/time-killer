package org.apache.flink.runtime.progress

import java.io.PrintWriter

import akka.actor.Actor
import org.apache.flink.runtime.progress.messages.ProgressMetricsReport
import org.slf4j.{Logger, LoggerFactory}

class ProgressMetricsLogger(numWindows: Int, parallelism: Int, winSize: Long, outputDir: String, bufferInterval: Integer) extends Actor {
  
  private var LOG: Logger = LoggerFactory.getLogger(classOf[ProgressMetricsLogger])

  val printWriter = new PrintWriter(s"$outputDir/$numWindows-$parallelism-$winSize-${System.currentTimeMillis()}.csv")
  printWriter.println("num_Windows,parallelism,win_Size,ctxid,step,operatorID,instanceID,window_Start,local_End,window_End,buffer_Interval")
  
  def receive(): Receive = {

    case report: ProgressMetricsReport =>
      printWriter.println(s"$numWindows,$parallelism,$winSize,${report.context.get(0)},${report.step},${report.operatorId}," +
        s"${report.instanceId},${report.startTS},${report.localEndTS},${report.endTS},$bufferInterval")
      printWriter.flush();
  }

  override def aroundPostStop(): Unit = {
    printWriter.flush()
    printWriter.close()
    super.aroundPostStop()
  }

}
