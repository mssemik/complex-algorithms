package semik.msc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.util.GraphGenerators
import semik.msc.betweenness.edmonds.EdmondsBC
import semik.msc.bfs.BFSShortestPath
import semik.msc.bfs.predicate.BFSVertexPredicate
import semik.msc.bfs.processor.BFSProcessor

/**
  * Created by mth on 3/15/17.
  */
object StartAlgorithm {

  def main(args: Array[String]): Unit = {

    val sConf = new SparkConf(true).setAppName("complex-algorithms")
         .setMaster("spark://192.168.1.21:7077")


    val sc = new SparkContext(sConf)
    sc.setCheckpointDir("/home/mth/sparkChpDir/")

    bfs(args(0).toInt, args(1).toInt, 2.5, 0.7)(sc)
  }

  def bfs(size: Int, part: Int, mu:Double, sigma: Double)(sc: SparkContext) = {

    val graph = GraphGenerators.logNormalGraph(sc, size, part, mu, sigma)

    val tt = new EdmondsBC[Long, Int](graph)

    tt.run
  }
}
