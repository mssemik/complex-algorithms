package semik.msc

import org.apache.spark.graphx.{Edge, PartitionStrategy}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import semik.msc.betweenness.partitioner.BalancedPartitioner
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

    val vertices = graph.edges.groupBy(e => e.srcId).map(e => (e._1, Map("edges" -> e._2)))

    val reaprtitionedVertices = vertices.partitionBy(new BalancedPartitioner(12))
    println("Vertices: " + reaprtitionedVertices.count())

    println("Number of part: " + reaprtitionedVertices.getNumPartitions)
    println("Size of parts:")
    reaprtitionedVertices.mapPartitionsWithIndex({ (idx, iter) => Array((idx, iter.size)).iterator}).collect().foreach(t => println("" + t._1 + ": " + t._2))

    //    println("Edges: " + graph.edges.count())



    val f = 1
  }
}
