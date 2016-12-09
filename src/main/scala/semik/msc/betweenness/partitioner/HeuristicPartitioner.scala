package semik.msc.betweenness.partitioner

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

/**
  * Created by mth on 12/7/16.
  */
object HeuristicPartitioner {

}

class BalancedPartitioner(paritions: Int, rdd: RDD) extends Partitioner {

  require(paritions > 0)

  override def numPartitions: Int = paritions

  override def getPartition(key: Any): Int =
    rdd.mapPartitionsWithIndex((index, iter) => Array((index, iter.size)).iterator, true).sortBy(p => p._2).first._1
}

class ChunkingPartitioner(partitions: Int, capacity: Int) extends Partitioner {
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = Math.ceil(partitions / capacity).toInt
}

class HashingPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = (key.hashCode % partitions) + 1
}

abstract class WeightedGreedyPartitioner extends Partitioner {

}
