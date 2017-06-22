package semik.msc.experiments

import ch.cern.sparkmeasure.StageMetrics
import org.apache.hadoop.fs.Path
import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import semik.msc.betweenness.edmonds.EdmondsBC
import semik.msc.betweenness.flow.CurrentFlowBC
import semik.msc.betweenness.flow.factory.FlowFactory
import semik.msc.betweenness.flow.generator.impl.RandomFlowGenerator
import semik.msc.betweenness.optimal.NearlyOptimalBC
import semik.msc.experiments.ExperimentsUtils.textEdgeToTuple

import scala.io.Source

/**
  * Created by mth on 5/29/17.
  */
class ExperimentManager {

  def runEdmondsAlgorithm(file: String, partitions: Int, iteration: Int) = {
    val sc = buildSparkContext(s"Edmonds_$file")
    val stageMeasure = new StageMetrics(SparkSession.builder().getOrCreate())

    stageMeasure.begin()

    val graph = readAlgorithmFromFile(sc, file, partitions)
    val edmondsAlgorithm = new EdmondsBC[Int, Int](graph)
    val result = edmondsAlgorithm.computeBC

    stageMeasure.end()

    println("###########   EDMONDS BC   ###########\n\n")
    stageMeasure.printReport()
    println(s"\n PARAMS: file: $file, partitions: $partitions, iteration: $iteration\n")

    val resultFileName = file.split("/").reverse.head + "_edmonds_result"
    result.map({ case (id, bc) => s"$id,$bc"}).saveAsTextFile("/home/msemik/experiments/result/"+resultFileName)

    removeCheckpointFiles(sc)
  }

  def runCurrentFlowAlgorithm(phi: Int, k: Int, epsilon: Double)(file: String, partitions: Int, iteration: Int) = {
    val sc = buildSparkContext(s"CFBC_$file")
    val stageMeasure = new StageMetrics(SparkSession.builder().getOrCreate())

    stageMeasure.begin()

    val graph = readAlgorithmFromFile(sc, file, partitions)
    val flowGenerator = new RandomFlowGenerator(phi, graph.ops.numVertices.toInt, k, new FlowFactory)
    val cfbcAlgorithm = new CurrentFlowBC[Int, Int](graph, flowGenerator)
    val result = cfbcAlgorithm.computeBC(phi, epsilon)

    stageMeasure.end()

    println("###########   CURRENT FLOW BC   ###########\n\n")
    stageMeasure.printReport()
//    stageMeasure.printAccumulables()
    println(s"\nPARAMS: file: $file, partitions: $partitions, iteration: $iteration, phi: $phi, k: $k, epsilon: $epsilon\n")
    val resultFileName = file.split("/").reverse.head + "_currentFlow_result"
    result.map({ case (id, bc) => s"$id,$bc"}).saveAsTextFile("/home/msemik/experiments/result/"+resultFileName)

    removeCheckpointFiles(sc)
  }

  def runOptimalAlgorithm(file: String, partitions: Int, iteration: Int) = {
    val sc = buildSparkContext(s"Optim_$file")
    val stageMeasure = new StageMetrics(SparkSession.builder().getOrCreate())

    stageMeasure.begin()

    val graph = readAlgorithmFromFile(sc, file, partitions)
    val optimAlgorithm = new NearlyOptimalBC[Int, Int](graph)
    val result = optimAlgorithm.computeBC

    stageMeasure.end()

    println("###########   NEARLY OPTIMAL BC   ###########\n\n")
    stageMeasure.printReport()
//    stageMeasure.printAccumulables()
    println(s"\nPARAMS: file: $file, partitions: $partitions, iteration: $iteration\n")
    val resultFileName = file.split("/").reverse.head + "_nearlyOptimal_result"
    result.map({ case (id, bc) => s"$id,$bc"}).saveAsTextFile("/home/msemik/experiments/result/"+resultFileName)

    removeCheckpointFiles(sc)
  }

  def saveExperimentResult(rdd: VertexRDD[Double], filename: String) =
    rdd.saveAsTextFile(filename)

  def readAlgorithmFromFile(sc: SparkContext, filename: String, partitions: Int) = {

    val edges = sc.textFile(filename, partitions).filter(!_.startsWith("#")).map(textEdgeToTuple)
    Graph.fromEdgeTuples(edges, 1, None, StorageLevel.MEMORY_ONLY, StorageLevel.MEMORY_ONLY)
  }

  def buildSparkContext(appName: String): SparkContext = {
    val sConf = new SparkConf()
      .setAppName(appName)

      .set("spark.executor.heartbeatInterval", "20s")
      .set("spark.driver.memory", "8g")
//        .set("spark.eventLog.enabled", "true")
//      .set("spark.eventLog.dir", "/home/msemik/logs")
      .set("spark.executor.memory", "5g")
      .set("spark.storage.memoryFraction", "0.9")

    val sc = new SparkContext(sConf)

    sc.setCheckpointDir("/home/msemik/checkpoint/")

    println(s"Checkpoint dir: ${sc.getCheckpointDir.getOrElse("None")}")
    sc
  }

  def printprop(name:String, c:SparkConf) = println(s"$name: ${c.get(name)}")

  def applyContextConfig(configFile: String, scf: SparkConf, sc: SparkContext): SparkConf = {
    val path = new Path("conf/spark-defaults.conf")
    val fs = path.getFileSystem(sc.hadoopConfiguration);


    val configLines = Source.fromFile(configFile).getLines().filterNot(_.startsWith("#"))

    configLines.foreach(line => {
      val cfg = line.trim().split("\\s+")
      if (cfg.length == 2) scf.set(cfg(0), cfg(1))
    })

    scf
  }

  def removeCheckpointFiles(sc: SparkContext): Unit = {
    val pathOpt = sc.getCheckpointDir.map(cpDir => new Path(cpDir))
    pathOpt match {
      case Some(path) =>
        val fs = path.getFileSystem(sc.hadoopConfiguration)
        fs.delete(path, true)
      case _ =>
    }
  }
}

object ExperimentManager {
  def apply(): ExperimentManager = new ExperimentManager()
}
