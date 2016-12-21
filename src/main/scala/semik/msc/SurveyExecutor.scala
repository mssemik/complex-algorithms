package semik.msc

import org.apache.spark.{SparkConf, SparkContext}
import semik.msc.betweenness.algorithms.HighBCExtraction
import semik.msc.betweenness.partitioner.{DeterministicGreedy, LinearGreedyFunction}
import semik.msc.loaders.mtx.MTXGraphLoader

/**
  * Created by mth on 12/5/16.
  */
object SurveyExecutor {

  def main(args : Array[String]) = {
    val sConf = new SparkConf(true).setAppName("complex-algorithms")
      .setMaster("spark://lenovoy50-70:7077")
      .set("spark.executor.memory", "512m")
      .set("spark.executor.instances", "4")

    implicit val sc = new SparkContext(sConf)

    startSurvey
  }

  def startSurvey(implicit sc:SparkContext) = {
    val parser = new MTXGraphLoader

    val graph = parser.loadDataFromFile("/media/mth/Data/repositories/Master Thesis code/soc-BlogCatalog1.mtx")

    val verts = graph.vertices
    val parts = Math.ceil(verts.countApprox(1000).getFinalValue().mean / 150).toInt

    val pertitioner = new DeterministicGreedy(parts, 200, LinearGreedyFunction)

    val alg = new HighBCExtraction(graph, pertitioner)

    alg.repartition
  }
}
