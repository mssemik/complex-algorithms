package semik.msc.partitioning

import org.apache.spark.graphx.Graph
import semik.msc.partitioning.config.JaBeJaConfig
import semik.msc.utils.JaBeJaUtils

import scala.util.Random

/**
  * Created by mth on 1/25/17.
  */
class JaBeJa(config: JaBeJaConfig) extends Serializable {


  def partitionGraph[VD, ED](graph: Graph[VD, ED]) = {
    val initColoredGraph = graph.mapVertices((_, _) => Random.nextInt(config.numOfPartition))

    val jaBeJaGraph = collectNeighboursColor(initColoredGraph)
  }

  def collectNeighboursColor[VD <: Int, ED](graph: Graph[VD, ED]) = {
    val messages = graph.aggregateMessages[Array[Int]](
      edgeContext => {
        edgeContext.sendToDst(JaBeJaUtils.buildArray(config.numOfPartition)(edgeContext.srcAttr, 1))
        edgeContext.sendToSrc(JaBeJaUtils.buildArray(config.numOfPartition)(edgeContext.dstAttr, 1))
      },
      (x, y) => JaBeJaUtils.sumArrays(x, y)(_ + _)
    )

    graph.outerJoinVertices(messages)((_, color, nbh) => (color, nbh))
  }
}
