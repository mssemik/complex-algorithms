package semik.msc.bfs.processor

import org.apache.spark.graphx.{EdgeTriplet, VertexId}
import semik.msc.processor.PregelProcessor

import scala.reflect.ClassTag
import scala.util.Random

/**
  * Created by mth on 3/15/17.
  */
class BFSProcessor[ED: ClassTag] extends PregelProcessor[List[VertexId], ED, List[VertexId]] {

  override def sendMessage(triplet: EdgeTriplet[List[VertexId], ED], bidirectional: Boolean): Iterator[(VertexId, List[VertexId])] = {

    def msgIterator(source: VertexId) = {
      val attr = triplet.otherVertexAttr(source)
      if (attr.nonEmpty) Iterator.empty else Iterator((triplet.otherVertexId(source), List(source)))
    }

    def hasParent(source: VertexId) = triplet.vertexAttr(source).nonEmpty

    val srcMsg = if (hasParent(triplet.srcId)) msgIterator(triplet.srcId) else Iterator.empty
    val dstMsg = if (bidirectional && hasParent(triplet.dstId)) msgIterator(triplet.dstId) else Iterator.empty
    srcMsg ++ dstMsg
  }

  override def mergeMessages(msg1: List[VertexId], msg2: List[VertexId]): List[VertexId] = msg1 ++ msg2

  override def initialMessage: List[VertexId] = List.empty
}
