package semik.msc.betweenness.optimal.struct.messages

import org.apache.spark.graphx.VertexId
import semik.msc.betweenness.optimal.struct.NOBFSVertex
import semik.msc.bfs.predicate.BFSVertexPredicate

/**
  * Created by mth on 5/9/17.
  */
class BFSBCExtendMessage(val source: VertexId, val distance: Double, val sigma: Int, val startRound: Int) extends NOMessage[VertexId] {
  override def content: VertexId = source

  override val isExpand: Boolean = true
}

object BFSBCExtendMessage extends Serializable {
  def apply(source: VertexId, distance: Double, sigma: Int, startRound: Int): BFSBCExtendMessage =
    new BFSBCExtendMessage(source, distance, sigma, startRound)

  def create(source: VertexId, vertex: NOBFSVertex): BFSBCExtendMessage =
    new BFSBCExtendMessage(source, vertex.distance + 1, vertex.sigma, vertex.startRound)
}
