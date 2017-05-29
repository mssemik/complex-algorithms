package semik.msc.betweenness.optimal.struct

import org.apache.spark.graphx.VertexId
import semik.msc.betweenness.optimal.struct.messages.DFSPointer

/**
  * Created by mth on 5/6/17.
  */
class NOVertex(val vertexId: VertexId,
               val bfsMap: Map[VertexId, NOBFSVertex],
               val pred: Option[VertexId],
               val succ: Option[Array[VertexId]],
               val dfsPointer: Option[DFSPointer]) extends Serializable {
  def setParent(idParent: VertexId) = NOVertex(vertexId, bfsMap, Some(idParent), succ, dfsPointer)

  def setPredecessorAndSuccessors(newPred: Option[VertexId], newSucc: Option[Array[VertexId]]) =
    NOVertex(vertexId, bfsMap, newPred, newSucc, dfsPointer)

  val isCompleted = pred.nonEmpty && succ.nonEmpty

  val leaf = succ.isEmpty

  lazy val bfsRoot = bfsMap.contains(vertexId)

  lazy val lowestSucc = succ.getOrElse(Array.empty).sorted.headOption

  lazy val eccentricity = if (bfsMap.isEmpty) 0 else bfsMap.map({ case (id, v) => v.distance}).max

  def withDfsPointer(pointer: Option[DFSPointer]) =
    NOVertex(vertexId, bfsMap, pred, succ, pointer)

  def update(bfsMap: Map[VertexId, NOBFSVertex] = bfsMap, succ: Option[Array[VertexId]] = succ, dfsPointer: Option[DFSPointer] = dfsPointer) =
    NOVertex(vertexId, bfsMap, pred, succ, dfsPointer)
}

object NOVertex extends Serializable {
  def apply(vertexId: VertexId,
            bfsMap: Map[VertexId, NOBFSVertex] = Map.empty,
            pred: Option[VertexId] = None,
            succ: Option[Array[VertexId]] = None,
            dfsPointer: Option[DFSPointer] = None): NOVertex = new NOVertex(vertexId, bfsMap, pred, succ, dfsPointer)
}
