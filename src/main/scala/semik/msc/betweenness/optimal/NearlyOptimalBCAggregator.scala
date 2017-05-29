package semik.msc.betweenness.optimal

import org.apache.spark.graphx.{EdgeContext, EdgeTriplet, Graph, VertexId}
import semik.msc.betweenness.optimal.struct.NOVertex
import semik.msc.betweenness.optimal.struct.messages.BCAggregationMessage
import semik.msc.pregel.Pregel

/**
  * Created by mth on 5/9/17.
  */
class NearlyOptimalBCAggregator[ED](graph: Graph[NOVertex, ED]) extends Serializable {

  def aggragateBC = {
    val diameter = graph.mapVertices((id, v) => v.eccentricity).vertices.max()._2

    val result = Pregel[NOVertex, NOVertex, ED, List[BCAggregationMessage]](
      graph,
      prepareVertices,
      applyMessages,
      sendMessages(diameter.toInt),
      _ ++ _, m => {}, 2
    )

    result.mapVertices((id, v) => v.bfsMap.map({ case (src, vbc) => if (src == id) 0 else vbc.psi * vbc.sigma.toDouble}).sum / 2).vertices
  }

  def prepareVertices(vertex: NOVertex) = vertex

  def applyMessages(round: Int)(vertexId: VertexId, vertex: NOVertex, messages: Option[List[BCAggregationMessage]]) =
    messages match {
      case Some(msg) =>
        val msgMap = msg.filterNot(_.source == vertexId).groupBy(_.source)
          .map({ case (src, m) => (src, vertex.bfsMap.get(src).get.updateBC(m.map(_.psi).sum)) })
        vertex.update(bfsMap = vertex.bfsMap ++ msgMap)
      case _ => vertex
    }

  def sendMessages(diameter: Int)(round: Int)(ctx: EdgeContext[NOVertex, ED, List[BCAggregationMessage]]) = {
    def sendAggregationMsg(triplet: EdgeTriplet[NOVertex, ED])(dstId: VertexId, send: (List[BCAggregationMessage]) => Unit) = {
      val srcAttr = triplet.otherVertexAttr(dstId)
      val dstAttr = triplet.vertexAttr(dstId)

      srcAttr.bfsMap.filter({ case (key, v) => v.startRound + diameter - v.distance == round && dstAttr.bfsMap.get(key).get.distance == (v.distance - 1) })
        .foreach({ case (key, v) => send(List(BCAggregationMessage(key, 1.0 / v.sigma.toDouble + v.psi))) })
    }

    val sendMsg = sendAggregationMsg(ctx.toEdgeTriplet) _
    sendMsg(ctx.srcId, ctx.sendToSrc)
    sendMsg(ctx.dstId, ctx.sendToDst)
  }
}
