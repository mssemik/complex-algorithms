package semik.msc.betweenness.partitioner

import org.apache.spark.graphx._

/**
  * Created by mth on 12/11/16.
  */
object HeuristicPartitionStrategy {

}

class BalancedStrategy(numOfPartition: Int) extends PartitionStrategy {

  val partitioningStats = new PartitoningStats(numOfPartition)

  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    val part = partitioningStats.getSmallestPartition
    partitioningStats.addElement(part)
    part
  }
}
