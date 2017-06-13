package semik.msc.betweenness.optimal.struct.messages

import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 6/11/17.
  */
class BFSBCConfirmMessage(val source: VertexId, child: VertexId) extends NOMessage[VertexId]{

  override def content: VertexId = child

  override def isConfirm: Boolean = true
}

object BFSBCConfirmMessage extends Serializable {
  def apply(source: VertexId, child: VertexId): BFSBCConfirmMessage = new BFSBCConfirmMessage(source, child)
}