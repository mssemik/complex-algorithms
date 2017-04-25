package semik.msc.path

import org.apache.spark.graphx.{Graph, VertexId}

/**
  * Created by mth on 3/8/17.
  */
class SimpleShortestPath[VD, ED](graph: Graph[VD, ED]) extends Serializable {

  def initGraph = {
    graph.mapVertices((vId, a) => new SPVertex(Map[VertexId, VertexId]((vId, vId)), List((vId, vId))))
  }

  def getShortestPaths = {
    initGraph.ops.pregel(List.empty[SPMessage])(
      (vId, v, msg) => {
        if (msg.nonEmpty) {
          val currMap = v.spMap.toList
          val msgList = msg.filter(m => v.spMap.get(m.dst).isEmpty).map(m => (m.dst, m.prev))
          new SPVertex(currMap ++ msgList toMap, msgList)
        } else v
      },
      tr => {
        val i1 = tr.srcAttr.toPass.map(m => new SPMessage(tr.srcId, m._1))
        val i2 = tr.dstAttr.toPass.map(m => new SPMessage(tr.dstId, m._1))
        Iterator((tr.dstId, i1), (tr.srcId, i2))
      },
      _ ++ _
      ).mapVertices((vid, v) => v.spMap)
  }

  class SPMessage(val prev: VertexId,
                  val dst: VertexId) extends Serializable

  class SPVertex(val spMap: Map[VertexId, VertexId], val toPass: List[(VertexId, VertexId)]) extends Serializable
}
