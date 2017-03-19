package semik.msc.betweenness.edmonds.processor

import org.apache.spark.graphx.{EdgeTriplet, VertexId}
import semik.msc.betweenness.edmonds.struct.EdmondsVertex
import semik.msc.processor.PregelProcessor

/**
  * Created by mth on 3/15/17.
  */
class EdmondsBCProcessor[ED] extends PregelProcessor[EdmondsVertex, ED, (List[VertexId], Int, Int)] {

  override def initialMessage: (List[VertexId], Int, Int) = (List.empty, -1, -1)

  override def mergeMessages(msg1: (List[VertexId], Int, Int), msg2: (List[VertexId], Int, Int)): (List[VertexId], Int, Int) = {
    assert(msg1._3 == msg2._3)
    (msg1._1 ++ msg2._1, msg1._2 + msg2._2, msg1._3)
  }

  override def sendMessage(triplet: EdgeTriplet[EdmondsVertex, ED], bidirectional: Boolean): Iterator[(VertexId, (List[VertexId], Int, Int))] = {

    def msgIterator(source: VertexId) = {
      val othAttr = triplet.otherVertexAttr(source)
      val thisAttr = triplet.vertexAttr(source)
      if (othAttr.rooted) Iterator.empty else Iterator((triplet.otherVertexId(source), (List(source), thisAttr.sigma, thisAttr.depth + 1)))
    }

    def hasParent(source: VertexId) = triplet.vertexAttr(source).rooted

    val srcMsg = if (hasParent(triplet.srcId)) msgIterator(triplet.srcId) else Iterator.empty
    val dstMsg = if (bidirectional && hasParent(triplet.dstId)) msgIterator(triplet.dstId) else Iterator.empty
    srcMsg ++ dstMsg
  }
}
