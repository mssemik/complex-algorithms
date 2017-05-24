package semik.msc.betweenness.optimal.struct.messages

import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 5/7/17.
  */
class DFSPointer(val source: VertexId, val next: Option[VertexId], val toSent: Boolean, val round: Long) extends NOMessage[VertexId] {
  override def content = source

  override val isDFSPointer = true

  val toRemove = toSent

  val returning = next.isEmpty

  def asToSent(n: Option[VertexId] = next) = DFSPointer(source, n, toSent = true, round + 1)

  def asWaiting(n: Option[VertexId]) = DFSPointer(source, n, toSent = false, round + 1)

  def asReturning = DFSPointer(source, None, toSent = true, round)
}

object DFSPointer extends Serializable {
  def apply(source: VertexId,
            next: Option[VertexId],
            toSent: Boolean,
            round: Long = 0
           ): DFSPointer = new DFSPointer(source, next, toSent, round)
}
