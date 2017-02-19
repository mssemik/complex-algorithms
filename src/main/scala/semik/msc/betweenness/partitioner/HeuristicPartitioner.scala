package semik.msc.betweenness.partitioner

import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}
import org.codehaus.jackson.map.ser.StdSerializers.UtilDateSerializer
import semik.msc.graph.{Edge, Vertex}

import scala.collection.mutable

/**
  * Created by mth on 12/7/16.
  */
object HeuristicPartitioner {

}

//trait HeuristicPartitioner extends Serializable {
//
//  val partStat = new PartitioningStats(numPartitions)
//
//  var partitionMap = Array.fill(numPartitions)(Set[VertexId]()).zipWithIndex
//
//  def numPartitions: Int
//
//  def computePartition(key: VertexId, nbh: Array[VertexId])
//
//  def sCtx: SparkContext
//
//  def computePartitions(graph: Graph[_, _]): RDD[Vertex] = {
//    val neighbourhood = graph.collectNeighborIds(EdgeDirection.Either)
//    for ((idx, v) <- neighbourhood.toLocalIterator) {
//      computePartition(idx, v)
//    }
//
//    val incomingEdges = graph.collectNeighbors(EdgeDirection.In).map({ case (vId, in) => (vId, in.map({ case (nId, d) => Edge(nId) })) })
//    val outgoingEdges = graph.collectNeighbors(EdgeDirection.Out).map({ case (vId, in) => (vId, in.map({ case (nId, d) => Edge(nId) })) })
//
//    val vertices = sCtx.parallelize(partitionMap.flatMap({ case (l, id) => l.map(v => (v, id)) }), graph.vertices.getNumPartitions).join(graph.vertices)
//    val verticesWithInNbh = vertices.fullOuterJoin(incomingEdges).map({ case (vId, (pId, o)) => (vId, (pId.get._1, o.getOrElse(Array()))) })
//    val verticesWithInAndOutNbh = verticesWithInNbh.fullOuterJoin(outgoingEdges).map({ case (vId, (x, y)) => (vId, (x.get._1, x.get._2, y.getOrElse(Array()))) })
//
//    vertices.partitions(0).
//
//    verticesWithInAndOutNbh.map({ case (vId, (pId, in, out)) => (pId, Vertex(vId, out, in)) }).partitionBy(new HashPartitioner(numPartitions)).values
//  }
//
//  def insertElementToPartition(idPart: Int, idVert: VertexId) = {
//    partStat.addElement(idPart)
//    partitionMap(idPart) = (partitionMap(idPart)._1 + idVert, idPart)
//  }
//}
//
//trait WeightedPartitioner extends HeuristicPartitioner {
//  def capacity: Int
//
//  def partitionSize(part: Int): Int
//}
//
//class BalancedPartitioner(partitions: Int)(implicit sc: SparkContext) extends HeuristicPartitioner {
//
//  require(partitions > 0)
//
//  def numPartitions: Int = partitions
//
//  def computePartition(key: VertexId, nbh: Array[VertexId]) = {
//    val part = partStat.getSmallestPartition
//    insertElementToPartition(part, key)
//  }
//
//  def sCtx = sc
//}
//
//
//class ChunkingPartitioner(partitions: Int, capacity: Int)(implicit sc: SparkContext) extends HeuristicPartitioner {
//
//  require(partitions > 0)
//
//  def numPartitions: Int = partitions
//
//  def computePartition(key: VertexId, nbh: Array[VertexId]) = {
//    val part = Math.ceil(partStat.getNumberOfElements / capacity).toInt
//    insertElementToPartition(part, key)
//  }
//
//  def sCtx = sc
//}
//
//class HashingPartitioner(partitions: Int)(implicit sc: SparkContext) extends HeuristicPartitioner {
//  override def numPartitions: Int = partitions
//
//  override def computePartition(key: VertexId, nbh: Array[VertexId]) = {
//    val part = (key.hashCode % partitions)
//    insertElementToPartition(part, key)
//  }
//
//  def sCtx = sc
//}
//
//class DeterministicGreedy(partitions: Int, val capacity: Int, w: ((Int) => Int, Int, Int) => Double)(implicit sc: SparkContext) extends WeightedPartitioner {
//
//  override def partitionSize(part: Int): Int = partStat.getPartitionSize(part)
//
//  override def numPartitions: Int = partitions
//
//  override def computePartition(key: VertexId, nbh: Array[VertexId]) = {
//    val candidatePartitions = heuristicEquation(key, nbh)
//    val partition = candidatePartitions.minBy(i => partStat.getPartitionSize(i))
//    insertElementToPartition(partition, key)
//  }
//
//  def sCtx = sc
//
//  def heuristicEquation(v: VertexId, nbh: Array[VertexId]): Seq[Int] =
//    partitionMap.map({ case (l, i) => (l.intersect(nbh.toSet).size * w(partitionSize, capacity, i), i) })
//      .groupBy({ case (ind, _) => ind }).maxBy({ case (ind, _) => ind })._2.map({ case (_, arr) => arr }).toSeq
//}
//
