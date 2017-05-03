package semik.msc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import semik.msc.betweenness.edmonds.EdmondsBC
import semik.msc.betweenness.flow.CurrentFlowBC
import semik.msc.betweenness.flow.factory.FlowFactory
import semik.msc.betweenness.flow.generator.impl.RandomFlowGenerator
import semik.msc.bfs.BFSShortestPath
import semik.msc.bfs.predicate.BFSVertexPredicate
import semik.msc.bfs.processor.BFSProcessor
import semik.msc.random.ctrw.ContinuousTimeRandomWalk

import scala.io.Source

/**
  * Created by mth on 3/15/17.
  */
object StartAlgorithm {

  def main(args: Array[String]): Unit = {

    val sConf = new SparkConf().setAppName("complex-algorithms")

    val sc = new SparkContext(sConf)

    sc.setCheckpointDir("hdfs://192.168.1.21:9000/chDir")

//    bfs(args(0).toInt, args(1).toInt, 0.8, 0.6)(sc)
//    bfsFile(args(0))(sc)
//    ctrw(args(0).toInt, args(1).toInt)(sc)
    cfbc(args(0).toInt, args(1).toInt, args(2).toDouble, args(3).toInt, args(4).toInt)(sc)
  }

  def bfs(size: Int, part: Int, mu:Double, sigma: Double)(sc: SparkContext) = {

    val graph = GraphGenerators.logNormalGraph(sc, size, part, mu, sigma)

    val tt = new EdmondsBC[Long, Int](graph)

    val bcVector = tt.computeBC

    bcVector.collect().foreach({ case (id, bc) => println("id: " + id + " => " + bc)})
  }

  def bfsFile(path: String)(sc: SparkContext) = {

    val graph = GraphLoader.edgeListFile(sc, path, true, 2)

    val tt = new EdmondsBC[Int, Int](graph)

    val bcVector = tt.computeBC

    bcVector.collect().foreach({ case (id, bc) => println("id: " + id + " => " + bc)})
  }

  def ctrw(size: Int, numOfPartitions: Int)(sc: SparkContext) = {
    val graph = GraphGenerators.logNormalGraph(sc, size, numOfPartitions)

    val rand = new ContinuousTimeRandomWalk[Long, Int](graph, 2.2)

    val randVrtices = rand.sampleVertices(15)

    randVrtices.mapValues(v => v.length).foreach({ case (id, l) => println(s"id: $id -> $l")})
  }

  def cfbc(size: Int, numOfPartition: Int, eps: Double, phi: Int, k: Int)(sc: SparkContext) = {
//    val graph = GraphGenerators.logNormalGraph(sc, size, numOfPartition)
    val graph = star(sc)

    val flowGenerator = new RandomFlowGenerator(phi, size, k, new FlowFactory)

    val core = new CurrentFlowBC(graph, flowGenerator)

    val res = core.computeBC(phi, eps)

    res.collect().foreach({ case (id, v) => println("id: " + id + " => " + v.bc)})
  }

  def star(sc:SparkContext) = {
    val l = sc.parallelize(for (i <- 2 to 10) yield new Edge(1, i, 0))
    Graph.fromEdges(l, 0)
  }
}
