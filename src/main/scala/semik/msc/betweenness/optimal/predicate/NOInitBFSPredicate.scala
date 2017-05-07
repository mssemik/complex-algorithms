package semik.msc.betweenness.optimal.predicate

import org.apache.spark.graphx.VertexId
import semik.msc.betweenness.optimal.struct.NOVertex
import semik.msc.betweenness.optimal.struct.messages.NOMessage
import semik.msc.predicate.vertex.VertexPredicate

/**
  * Created by mth on 5/6/17.
  */
class NOInitBFSPredicate extends VertexPredicate[NOVertex, List[NOMessage[VertexId]]] {

  override def getInitialData(vertexId: VertexId, attr: NOVertex): (VertexId) => NOVertex =
    (id: VertexId) => if (id == vertexId) attr.setParent(id) else attr

  override def applyMessages(vertexId: VertexId, vertex: NOVertex, message: List[NOMessage[VertexId]]): NOVertex =
    if (vertex.isCompleted) vertex else updateVertex(vertex, message)


  def updateVertex(vertex: NOVertex, messages: List[NOMessage[VertexId]]) = {
    val parent = extractParrent(vertex, messages)
    val succ = extractSuccessors(vertex, messages)
    vertex.setPredecessorAndSuccessors(parent, succ)
  }

  def extractParrent(vertex: NOVertex, messages: List[NOMessage[VertexId]]) = {
    vertex.pred match {
      case Some(pred) => vertex.pred
      case None =>
        val expandMsg = messages.filter(_.isExpand).map(_.content)
        expandMsg.headOption
    }
  }

  def extractSuccessors(vertex: NOVertex, messages: List[NOMessage[VertexId]]) =
    vertex.succ match {
      case Some(arr) => vertex.succ
      case None =>
        val confirmMsg = messages.filter(_.isConfirm).map(_.content)
        if (confirmMsg.nonEmpty) Some(confirmMsg.toArray) else None
    }
}
