package semik.msc.betweenness.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}
import semik.msc.graph.Vertex

/**
  * Created by mth on 12/7/16.
  */
object HeuristicPartitioner {

}

trait HeuristicPartitioner extends Serializable {

  val partStat = new PartitioningStats(numPartitions)

  def numPartitions: Int

  def computePartition(key: Vertex)

  def computePartitions(rdd: RDD[(Long, Vertex)]): RDD[(Long, Vertex)]
}

trait WeightedPartitioner extends HeuristicPartitioner {
  def capacity: Int

  def partitionSize(part: Int): Int
}

//class BalancedPartitioner(partitions: Int) extends HeuristicPartitioner {
//
//  require(partitions > 0)
//
//  def numPartitions: Int = partitions
//
//  def getPartition(key: Vertex): Int = {
//    val part = partStat.getSmallestPartition
//    partStat.addElement(part)
//    part
//  }
//
//  def computePartitions(rdd: RDD[(Long, Vertex)]) : RDD[(Int, (Long, Vertex))] = {
//  }
//}
//
//class ChunkingPartitioner(partitions: Int, capacity: Int) extends HeuristicPartitioner {
//
//  require(partitions > 0)
//
//  def numPartitions: Int = partitions
//
//  def getPartition(key: Vertex): Int = {
//    val part = Math.ceil(partStat.getNumberOfElements / capacity).toInt
//    partStat.addElement(part)
//    part
//  }
//}
//
//class HashingPartitioner(partitions: Int) extends HeuristicPartitioner {
//  override def numPartitions: Int = partitions
//
//  override def getPartition(key: Vertex): Int = (key.hashCode % partitions) + 1
//}

class DeterministicGreedy(partitions: Int, val capacity: Int, w: ((Int) => Int, Int, Int) => Double)(implicit sc: SparkContext) extends WeightedPartitioner {

//  var partitionMap = sc.parallelize((0 to partitions - 1), partitions / 10).map(idx => (idx, List[Long]()))
  var partitionMap = Array.fill(partitions)(List[Long]()).zipWithIndex

  var counter: Int = 0

  override def partitionSize(part: Int): Int = partStat.getPartitionSize(part)

  override def numPartitions: Int = partitions

  override def computePartition(key: Vertex) = {
    val parts = P(key)

    val part = if (parts.size > 1) parts.minBy(i => partStat.getPartitionSize(i)) else parts.head

    partStat.addElement(part)

    counter = counter + 1

    if (counter % 500 == 0)
      println(counter)

    partitionMap.update(part, (key.id :: partitionMap(part)._1, part))

//    partitionMap = partitionMap.map({ case (list, pId) => (if (pId == part) key.id :: list else list, pId) })
  }

  def computePartitions(rdd: RDD[(Long, Vertex)]): RDD[(Long, Vertex)] = {
    for ((idx, v) <- rdd.toLocalIterator) {
      computePartition(v)
    }

    sc.parallelize(partitionMap.flatMap({ case (l, id) => l.map(v => (v, id)) }), partitions / 10).join(rdd).map({ case (vId, (pId, v)) => (pId, (vId, v)) }).repartition(partitions).values
  }

  def P(v: Vertex): Seq[Int] =
    partitionMap.map({ case (l, i) => (l.intersect(v.incomingEdges.keySet.toSeq ++ v.outgoingEdges.keySet.toSeq).size * w(partitionSize, capacity, i), i) })
        .groupBy( { case (ind, i) => ind }).maxBy( { case (ind, i) => ind } )._2.map( { case (ind1, arr) => arr } ).toSeq

//    partitionMap.map({ case (i, l) => (l.intersect(v.incomingEdges.keySet.toSeq ++ v.outgoingEdges.keySet.toSeq).size * w(partitionSize, capacity, i), i) })
//      .groupByKey().sortBy({ case (l, i) => l }, false).first()._2.toSeq

}

