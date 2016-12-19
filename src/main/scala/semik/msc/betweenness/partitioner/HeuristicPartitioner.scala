package semik.msc.betweenness.partitioner

import org.apache.spark.Partitioner

/**
  * Created by mth on 12/7/16.
  */
object HeuristicPartitioner {

}

class BalancedPartitioner(partitions: Int) extends Serializable {

  require(partitions > 0)

  val partStat = new PartitoningStats(partitions)

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = {
    val part = partStat.getSmallestPartition
    partStat.addElement(part)
    part
  }
}

class ChunkingPartitioner(partitions: Int, capacity: Int) extends Partitioner {

  require(partitions > 0)

  val partStat = new PartitoningStats(partitions)

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    val part = Math.ceil(partStat.getNumberOfElements / capacity).toInt
    partStat.addElement(part)
    part
  }
}

class HashingPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = (key.hashCode % partitions) + 1
}

trait WeightedGreedyPartitioner extends Partitioner {

}
