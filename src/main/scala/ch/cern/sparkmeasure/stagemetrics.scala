package ch.cern.sparkmeasure

import org.apache.spark.scheduler._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer
import org.apache.log4j.LogManager

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

case class StageVals (jobId: Int, stageId: Int, name: String,
                 submissionTime: Long, completionTime: Long, stageDuration: Long, numTasks: Int,
                 executorRunTime: Long, executorCpuTime: Long,
                 executorDeserializeTime: Long, executorDeserializeCpuTime: Long,
                 resultSerializationTime: Long, jvmGCTime: Long, resultSize: Long, numUpdatedBlockStatuses: Int,
                 diskBytesSpilled: Long, memoryBytesSpilled: Long, peakExecutionMemory: Long, recordsRead: Long,
                 bytesRead: Long, recordsWritten: Long, bytesWritten: Long,
                 shuffleFetchWaitTime: Long, shuffleTotalBytesRead: Long, shuffleTotalBlocksFetched: Long,
                 shuffleLocalBlocksFetched: Long, shuffleRemoteBlocksFetched: Long, shuffleWriteTime: Long,
                 shuffleBytesWritten: Long, shuffleRecordsWritten: Long
                )

case class StageAccumulablesInfo (jobId: Int, stageId: Int, submissionTime: Long, completionTime: Long,
                                  accId: Long, name: String, value: Long)

class StageInfoRecorderListener extends SparkListener {

  val stageMetricsData: ListBuffer[StageVals] = ListBuffer.empty[StageVals]
  val accumulablesMetricsData: ListBuffer[StageAccumulablesInfo] = ListBuffer.empty[StageAccumulablesInfo]
  val StageIdtoJobId: collection.mutable.HashMap[Int, Int] = collection.mutable.HashMap.empty[Int, Int]

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    jobStart.stageIds.foreach(stageId => StageIdtoJobId += (stageId -> jobStart.jobId))
  }

  /**
   * This methods fires at the end of the stage and collects metrics flattened into the stageMetricsData ListBuffer
   * Note all times are in ms, cpu time and shufflewrite are originally in nanosec, thus in the code are divided by 1e6
   */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageInfo = stageCompleted.stageInfo
    val taskMetrics = stageInfo.taskMetrics
    val jobId = StageIdtoJobId(stageInfo.stageId)
    val currentStage = StageVals(jobId, stageInfo.stageId, stageInfo.name,
        stageInfo.submissionTime.getOrElse(0L), stageInfo.completionTime.getOrElse(0L),
      stageInfo.completionTime.getOrElse(0L) - stageInfo.submissionTime.getOrElse(0L),
      stageInfo.numTasks, taskMetrics.executorRunTime, 0,/*taskMetrics.executorCpuTime / 1000000,*/
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
    stageMetricsData += currentStage

    /** Collect data from accumulators, with additional care to keep only numerical values */
    stageInfo.accumulables.foreach(acc => try {
      val value = acc._2.value.getOrElse(0L).asInstanceOf[Long]
      val name = acc._2.name.getOrElse("")
      val currentAccumulablesInfo = StageAccumulablesInfo(jobId, stageInfo.stageId,
          stageInfo.submissionTime.getOrElse(0L), stageInfo.completionTime.getOrElse(0L), acc._1, name, value)
      accumulablesMetricsData += currentAccumulablesInfo
    }
    catch {
      case ex: ClassCastException => None
    }
    )
  }
}


case class StageMetrics(sparkSession: SparkSession) {

  lazy val logger = LogManager.getLogger("StageMetrics")

  /** This inserts the custom Spark Listener into the live Spark Context */
  val listenerStage = new StageInfoRecorderListener
  sparkSession.sparkContext.addSparkListener(listenerStage)

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

  /** Move data recorded from the custom listener into a DataFrame and register it as a view for easier processing */
  def createStageMetricsDF(nameTempView: String = "PerfStageMetrics"): DataFrame = {
    import sparkSession.implicits._
    val resultDF = listenerStage.stageMetricsData.toDF
    resultDF.createOrReplaceTempView(nameTempView)
    logger.warn(s"Stage metrics data refreshed into temp view $nameTempView")
    resultDF
  }

  def createAccumulablesDF(nameTempView: String = "AccumulablesStageMetrics"): DataFrame = {
    import sparkSession.implicits._
    val resultDF = listenerStage.accumulablesMetricsData.toDF
    resultDF.createOrReplaceTempView(nameTempView)
    logger.warn(s"Accumulables metrics data refreshed into temp view $nameTempView")
    resultDF
  }

  /** for internal metrics summ all the values, for the accumulables compute max value for eax accId and name */
  def printAccumulables(): Unit = {
    createAccumulablesDF("AccumulablesStageMetrics")
    val internalMetricsDf = sparkSession.sql(s"select name, sum(value) " +
      s"from AccumulablesStageMetrics " +
      s"where submissionTime >= $beginSnapshot and completionTime <= $endSnapshot " +
      s"and name like 'internal.metric%' " +
      s"group by name")
    println("\nAggregated Spark accumulables of type internal.metric:")
    internalMetricsDf.show(200, false)

    val otherAccumulablesDf = sparkSession.sql(s"select accId, name, max(value) as endValue " +
      s"from AccumulablesStageMetrics " +
      s"where submissionTime >= $beginSnapshot and completionTime <= $endSnapshot " +
      s"and name not like 'internal.metric%'" +
      s"group by accId, name")
    println("\nAggregated Spark accumulables of type != internal.metric:")
    otherAccumulablesDf.show(200,false)
  }

  /** Custom aggreagations and post-processing of the metrics data */
  def printReport(): Unit = {

    createStageMetricsDF("PerfStageMetrics")
    val aggregateDF = sparkSession.sql(s"select count(*) numStages, sum(numTasks), " +
      s"max(completionTime) - min(submissionTime) as elapsedTime, sum(stageDuration), sum(executorRunTime), " +
      s"sum(executorCpuTime), sum(executorDeserializeTime), sum(executorDeserializeCpuTime), " +
      s"sum(resultSerializationTime), sum(jvmGCTime), sum(shuffleFetchWaitTime), sum(shuffleWriteTime), " +
      s"max(resultSize), sum(numUpdatedBlockStatuses), sum(diskBytesSpilled), sum(memoryBytesSpilled), " +
      s"max(peakExecutionMemory), sum(recordsRead), sum(bytesRead), sum(recordsWritten), sum(bytesWritten), " +
      s" sum(shuffleTotalBytesRead), sum(shuffleTotalBlocksFetched), sum(shuffleLocalBlocksFetched), " +
      s"sum(shuffleRemoteBlocksFetched), sum(shuffleBytesWritten), sum(shuffleRecordsWritten) " +
      s"from PerfStageMetrics " +
      s"where submissionTime >= $beginSnapshot and completionTime <= $endSnapshot")
    val results = aggregateDF.take(1)

    println(s"\nScheduling mode = ${sparkSession.sparkContext.getSchedulingMode.toString}")
    println(s"Spark Contex default degree of parallelism = ${sparkSession.sparkContext.defaultParallelism}")
    println("Aggregated Spark stage metrics:")

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

  /** Helper method to save data, we expect to have small amounts of data so collapsing to 1 partition seems OK */
  def saveData(df: DataFrame, fileName: String, fileFormat: String = "json") = {
    df.orderBy("jobId", "stageId").repartition(1).write.format(fileFormat).save(fileName)
    logger.warn(s"Stage metric data saved into $fileName using format=$fileFormat")
  }

}
