package semik.msc

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.{SparkConf, SparkContext}
import semik.msc.random.RandomWeightWalk
import semik.msc.random.config.RandomWeightWalkConfig
//import semik.msc.betweenness.partitioner.JaBeJaPartitioner
import semik.msc.loaders.mtx.MTXGraphLoader

/**
  * Created by mth on 12/5/16.
  */
object SurveyExecutor {

  def main(args : Array[String]) = {
    val sConf = new SparkConf(true).setAppName("complex-algorithms")
      .setMaster("spark://192.168.1.21:7077")


    implicit val sc = new SparkContext(sConf)
    sc.setCheckpointDir("/home/mth/sparkChpDir/")

    startSurvey(100, 0.02)
  }

  def startSurvey(size: Int, qn: Double)(implicit sc:SparkContext) = {
    val graph = GraphGenerators.logNormalGraph(sc, size, 16, 2.5)

    val dd = new RandomWeightWalk(qn)
    val pp = dd.prepareWalk(graph)

    println("Max W: " + pp.vertices.map(v => v._2.vertexWeight).max)
    println("Min W: " + pp.vertices.map(v => v._2.vertexWeight).min)
    println("Mean W: " + pp.vertices.map(v => v._2.vertexWeight).mean())

  }
}
