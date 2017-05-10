package semik.msc.betweenness.optimal.struct.messages

import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 5/9/17.
  */
class BCAggregationMessage(val source: VertexId, val psi: Double) extends NOMessage[VertexId] {
  override val content: VertexId = source
}

object BCAggregationMessage extends Serializable {
  def apply(source: VertexId, psi: Double): BCAggregationMessage = new BCAggregationMessage(source, psi)
}