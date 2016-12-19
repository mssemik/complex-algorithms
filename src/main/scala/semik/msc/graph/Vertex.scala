package semik.msc.graph

/**
  * Created by mth on 12/18/16.
  */
class Vertex(val id: Long, val outgoingEdges: Map[Long, Edge], val incomingEdges: Map[Long, Edge]) extends Serializable {
  var attr: Map[String, Any] = Map()
}

object Vertex {
  def apply(id: Long, outgoingEdges: Seq[Edge], incomingEdges: Seq[Edge]) = {
    val outMap: Map[Long, Edge] = outgoingEdges.map(e => (e.vertId, e)).toMap
    val inMap: Map[Long, Edge] = incomingEdges.map(e => (e.vertId, e)).toMap
    new Vertex(id, outMap, inMap)
  }
}
