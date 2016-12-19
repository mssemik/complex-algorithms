package semik.msc.betweenness.partitioner

import org.roaringbitmap.buffer.MutableRoaringArray

/**
  * Created by mth on 12/11/16.
  */
class PartitoningStats(val numPartition: Int) extends Serializable {

  val partitionSizes = Array.fill(numPartition)(0).zipWithIndex

  var mappedVertex: Map[Long, Int]

  def addElement(part: Int) = {
    require(part < numPartition)

    val item = partitionSizes(part)
    partitionSizes(part) = (item._1 + 1, item._2)
  }

  def getPartitionSize(part: Int) = partitionSizes(part) _1

  def getSmallestPartition = partitionSizes.minBy(_._1) _2

  def getNumberOfElements = partitionSizes.aggregate(0)((v, t) => v + t._1, (v1, v2) => v1 + v2)
}
