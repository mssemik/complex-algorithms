package ch.cern.sparkmeasure

import org.apache.log4j.LogManager
import org.apache.spark.scheduler._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
 * Spark Measure package: proof-of-concept tool for measuring Spark performance metrics
 *   This is based on using Spark Listeners as data source and collecting metrics in a ListBuffer
 *   The list buffer is then transformed into a DataFrame for analysis
 *
 *  Stage Metrics: collects and aggregates metrics at the end of each stage
 *  Task Metrics: collects data at task granularity
 *
 * Use modes:
 *   Interactive mode from the REPL
 *   Flight recorder mode: records data and saves it for later processing
 *
 * Supported languages:
 *   The tool is written in Scala, but it can be used both from Scala and Python
 *
 * Example usage for stage metrics:
 * val stageMetrics = new ch.cern.sparkmeasure.StageMetrics(spark)
 * stageMetrics.runAndMeasure(spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show)
 *
 * for task metrics:
 * val taskMetrics = new ch.cern.sparkmeasure.TaskMetrics(spark)
 * spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
 * val df = taskMetrics.createTaskMetricsDF()
 *
 * To use in flight recorder mode add:
 * --conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderStageMetrics
 *
 * Created by Luca.Canali@cern.ch, March 2017
 *
 */

case class TaskVals(jobId: Int, stageId: Int, index: Long, launchTime: Long, finishTime: Long,
                duration: Long, schedulerDelay: Long, executorId: String, host: String, taskLocality: Int,
                speculative: Boolean, gettingResultTime: Long, successful: Boolean,
                executorRunTime: Long, executorCpuTime: Long,
                executorDeserializeTime: Long, executorDeserializeCpuTime: Long,
                resultSerializationTime: Long, jvmGCTime: Long, resultSize: Long, numUpdatedBlockStatuses: Int,
                diskBytesSpilled: Long, memoryBytesSpilled: Long, peakExecutionMemory: Long, recordsRead: Long,
                bytesRead: Long, recordsWritten: Long, bytesWritten: Long,
                shuffleFetchWaitTime: Long, shuffleTotalBytesRead: Long, shuffleTotalBlocksFetched: Long,
                shuffleLocalBlocksFetched: Long, shuffleRemoteBlocksFetched: Long, shuffleWriteTime: Long,
                shuffleBytesWritten: Long, shuffleRecordsWritten: Long)

// repetition of the case class, with modification TO FIX
case class TaskAccumulablesInfo(jobId: Int, stageId: Int, taskId: Long, submissionTime: Long, finishTime: Long,
                                accId: Long, name: String, value: Long)

class TaskInfoRecorderListener extends SparkListener {

  val taskMetricsData: ListBuffer[TaskVals] = ListBuffer.empty[TaskVals]
  val accumulablesMetricsData: ListBuffer[TaskAccumulablesInfo] = ListBuffer.empty[TaskAccumulablesInfo]
  val StageIdtoJobId: collection.mutable.HashMap[Int, Int] = collection.mutable.HashMap.empty[Int, Int]

  def encodeTaskLocality(taskLocality: TaskLocality.TaskLocality): Int = {
    taskLocality match {
      case TaskLocality.PROCESS_LOCAL => 0
      case TaskLocality.NODE_LOCAL => 1
      case TaskLocality.RACK_LOCAL => 2
      case TaskLocality.NO_PREF => 3
      case TaskLocality.ANY => 4
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    jobStart.stageIds.foreach(stageId => StageIdtoJobId += (stageId -> jobStart.jobId))
  }

  /**
   * This methods fires at the end of the Task and collects metrics flattened into the taskMetricsData ListBuffer
   * Note all times are in ms, cpu time and shufflewrite are originally in nanosec, thus in the code are divided by 1e6
   */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val taskInfo = taskEnd.taskInfo
    val taskMetrics = taskEnd.taskMetrics
    val gettingResultTime = {
      if (taskInfo.gettingResultTime == 0L) 0L
      else taskInfo.finishTime - taskInfo.gettingResultTime
    }
    val duration = taskInfo.finishTime - taskInfo.launchTime
    val jobId = StageIdtoJobId(taskEnd.stageId)
    val currentTask = TaskVals(jobId, taskEnd.stageId, taskInfo.taskId, taskInfo.launchTime,
      taskInfo.finishTime, duration,
      math.max(0L, duration - taskMetrics.executorRunTime - taskMetrics.executorDeserializeTime -
        taskMetrics.resultSerializationTime - gettingResultTime),
      taskInfo.executorId, taskInfo.host, encodeTaskLocality(taskInfo.taskLocality),
      taskInfo.speculative, gettingResultTime, taskInfo.successful,
      taskMetrics.executorRunTime, 0, /*taskMetrics.executorCpuTime / 1000000,*/
      taskMetrics.executorDeserializeTime, 0, /*taskMetrics.executorDeserializeCpuTime / 1000000,*/
      taskMetrics.resultSerializationTime, taskMetrics.jvmGCTime, taskMetrics.resultSize,
      taskMetrics.updatedBlockStatuses.length, taskMetrics.diskBytesSpilled, taskMetrics.memoryBytesSpilled,
      taskMetrics.peakExecutionMemory,
      taskMetrics.inputMetrics.recordsRead, taskMetrics.inputMetrics.bytesRead,
      taskMetrics.outputMetrics.recordsWritten, taskMetrics.outputMetrics.bytesWritten,
      taskMetrics.shuffleReadMetrics.fetchWaitTime, taskMetrics.shuffleReadMetrics.totalBytesRead,
      taskMetrics.shuffleReadMetrics.totalBlocksFetched, taskMetrics.shuffleReadMetrics.localBlocksFetched,
      taskMetrics.shuffleReadMetrics.remoteBlocksFetched,
      taskMetrics.shuffleWriteMetrics.writeTime / 1000000, taskMetrics.shuffleWriteMetrics.bytesWritten,
      taskMetrics.shuffleWriteMetrics.recordsWritten
    )
    taskMetricsData += currentTask

    /** Collect data from accumulators, with additional care to keep only numerical values */
    taskInfo.accumulables.foreach(acc => try {
      val value = acc.value.getOrElse(0L).asInstanceOf[Long]
      val name = acc.name.getOrElse("")
      val currentAccumulablesInfo = TaskAccumulablesInfo(jobId, taskEnd.stageId, taskInfo.taskId,
        taskInfo.launchTime, taskInfo.finishTime ,acc.id, name, value)
      accumulablesMetricsData += currentAccumulablesInfo
    }
    catch {
      case ex: ClassCastException => None
    }
    )

  }
}

case class TaskMetrics(sparkSession: SparkSession) {

  lazy val logger = LogManager.getLogger("TaskMetrics")

  /** This inserts the custom Spark Listener into the live Spark Context */
  val listenerTask = new TaskInfoRecorderListener
  sparkSession.sparkContext.addSparkListener(listenerTask)

  /** Variables used to store the start and end time of the period of interest for the metrics report */
  var beginSnapshot: Long = 0L
  var endSnapshot: Long = 0L

  def begin(): Long = {
    beginSnapshot = System.currentTimeMillis()
    beginSnapshot
  }

  def end(): Long = {
    endSnapshot = System.currentTimeMillis()
    endSnapshot
  }

  def createAccumulablesDF(nameTempView: String = "AccumulablesTaskMetrics"): DataFrame = {
    import sparkSession.implicits._
    val resultDF = listenerTask.accumulablesMetricsData.toDF
    resultDF.createOrReplaceTempView(nameTempView)
    logger.warn(s"Accumulables metrics data refreshed into temp view $nameTempView")
    resultDF
  }

  def printAccumulables(): Unit = {
    val accumulableTaskMetrics="AccumulablesTaskMetrics"
    val aggregatedAccumulables="AggregatedAccumulables"
    createAccumulablesDF(accumulableTaskMetrics)

    sparkSession.sql(s"select accId, name, max(value) as endValue " +
      s"from $accumulableTaskMetrics " +
      s"where submissionTime >= $beginSnapshot and finishTime <= $endSnapshot " +
      s"group by accId, name").createOrReplaceTempView(aggregatedAccumulables)

    val internalMetricsDf = sparkSession.sql(s"select name, sum(endValue) " +
      s"from $aggregatedAccumulables " +
      s"where name like 'internal.metric%' " +
      s"group by name")
    println("\nAggregated Spark accumulables of type internal.metric:")
    internalMetricsDf.show(200, false)

    val otherAccumulablesDf = sparkSession.sql(s"select accId, name, endValue " +
      s"from $aggregatedAccumulables " +
      s"where name not like 'internal.metric%' ")
    println("\nSpark accumulables by accId of type != internal.metric:")
    otherAccumulablesDf.show(200, false)
  }


  def createTaskMetricsDF(nameTempView: String = "PerfTaskMetrics"): DataFrame = {
    import sparkSession.implicits._
    val resultDF = listenerTask.taskMetricsData.toDF
    resultDF.createOrReplaceTempView(nameTempView)
    logger.warn(s"Stage metrics data refreshed into temp view $nameTempView")
    resultDF
  }

  def printReport(): Unit = {

    createTaskMetricsDF("PerfTaskMetrics")
    val aggregateDF = sparkSession.sql(s"select count(*) numtasks, " +
      s"max(finishTime) - min(launchTime) as elapsedTime, sum(duration), sum(schedulerDelay), sum(executorRunTime), " +
      s"sum(executorCpuTime), sum(executorDeserializeTime), sum(executorDeserializeCpuTime), " +
      s"sum(resultSerializationTime), sum(jvmGCTime), sum(shuffleFetchWaitTime), sum(shuffleWriteTime), " +
      s"sum(gettingResultTime), " +
      s"max(resultSize), sum(numUpdatedBlockStatuses), sum(diskBytesSpilled), sum(memoryBytesSpilled), " +
      s"max(peakExecutionMemory), sum(recordsRead), sum(bytesRead), sum(recordsWritten), sum(bytesWritten), " +
      s" sum(shuffleTotalBytesRead), sum(shuffleTotalBlocksFetched), sum(shuffleLocalBlocksFetched), " +
      s"sum(shuffleRemoteBlocksFetched), sum(shuffleBytesWritten), sum(shuffleRecordsWritten) " +
      s"from PerfTaskMetrics " +
      s"where launchTime >= $beginSnapshot and finishTime <= $endSnapshot")
    val results = aggregateDF.take(1)

    println(s"\nScheduling mode = ${sparkSession.sparkContext.getSchedulingMode.toString}")
    println(s"Spark Contex default degree of parallelism = ${sparkSession.sparkContext.defaultParallelism}")
    println("Aggregated Spark task metrics:")

    (aggregateDF.columns zip results(0).toSeq).foreach(r => {
      val name = r._1.toLowerCase()
      val value = r._2.asInstanceOf[Long]
      println(name + " = " + value.toString + {
        if (name.contains("time") || name.contains("duration")) {
          " (" + Utils.formatDuration(value) + ")"
        }
        else if (name.contains("bytes") && value > 1000L) {
          " (" + Utils.formatBytes(value) + ")"
        }
        else ""
      })
    })
  }

  /** Shortcut to run and measure the metrics for Spark execution, built after spark.time() */
  def runAndMeasure[T](f: => T): T = {
    this.begin()
    val startTime = System.nanoTime()
    val ret = f
    val endTime = System.nanoTime()
    this.end()
    println(s"Time taken: ${(endTime - startTime) / 1000000} ms")
    printReport()
    ret
  }

  /** helper method to save data, we expect to have moderate amounts of data so collapsing to 1 partition seems OK */
  def saveData(df: DataFrame, fileName: String, fileFormat: String = "json") = {
    df.orderBy("jobId", "stageId", "index").repartition(1).write.format(fileFormat).save(fileName)
    logger.warn(s"Task metric data saved into $fileName using format=$fileFormat")
  }

}
