package semik.msc.random.ctrw.processor

import org.apache.spark.graphx._
import semik.msc.factory.Factory
import semik.msc.random.ctrw.struct.{CTRWMessage, CTRWVertex}
import semik.msc.util.GraphSimplifier

/**
  * Created by mth on 4/13/17.
  */
class CTRWProcessor[VD, ED](graph: Graph[VD, ED], factory: Factory[CTRWVertex, CTRWMessage]) extends Serializable {

  lazy val initGraph = prepareRawGraph

  private def prepareRawGraph = {
    val simpleGraph = GraphSimplifier.simplifyGraph(graph)((m, _) => m)
    val nbsIds = simpleGraph.ops.collectNeighborIds(EdgeDirection.Either)
    simpleGraph.outerJoinVertices(nbsIds)((id, _, nbs) => CTRWVertex(id, nbs.getOrElse(Array.empty), Array.empty, initialized = false))
  }

  def createInitMessages(sampleSize: Int)(vertex: CTRWVertex) = {
    val msg = for (i <- 0 until sampleSize) yield factory.create(vertex)
    CTRWVertex(vertex.id, vertex.neighbours, msg.toArray)
  }

  def sendMessage(triplet: EdgeTriplet[CTRWVertex, ED]) = {

    def messagesTo(dest: VertexId) = {
      def messages = triplet.otherVertexAttr(dest).messages

      messages filter (_.nextVertex.contains(dest)) toList
    }

    Iterator((triplet.srcId, messagesTo(triplet.srcId))) ++ Iterator((triplet.dstId, messagesTo(triplet.dstId)))
  }

  def sendMessageCtx(edgeContext: EdgeContext[CTRWVertex, _, List[CTRWMessage]]) = {
    val triplet = edgeContext.toEdgeTriplet

    def messagesTo(dest: VertexId) = {
      def messages = triplet.otherVertexAttr(dest).messages

      messages filter (_.nextVertex.contains(dest)) toList
    }

    def send(msg: List[CTRWMessage], f: (List[CTRWMessage]) => Unit) =
      if (msg.nonEmpty) f(msg)

    send(messagesTo(edgeContext.srcId), edgeContext.sendToSrc)
    send(messagesTo(edgeContext.dstId), edgeContext.sendToDst)
  }

  def mergeMessages(msg1: List[CTRWMessage], msg2: List[CTRWMessage]) = msg1 ++ msg2

  def applyMessages(vertexId: VertexId, data: CTRWVertex, messages: List[CTRWMessage]) = {
    val newMessages = messages map (factory.correct(data, _))
    val keptMessages = data.messages filter (_.nextVertex.isEmpty)
    CTRWVertex(vertexId, data.neighbours, newMessages.toArray ++ keptMessages)
  }
}
