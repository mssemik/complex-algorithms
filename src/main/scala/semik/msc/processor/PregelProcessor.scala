package semik.msc.processor

import org.apache.spark.graphx.{EdgeTriplet, VertexId}

/**
  * Created by mth on 3/15/17.
  */
trait PregelProcessor[VD, ED, MD] extends Serializable {
  def initialMessage: MD
  def sendMessage(triplet: EdgeTriplet[VD, ED], bidirectional: Boolean): Iterator[(VertexId, MD)]
  def mergeMessages(msg1: MD, msg2: MD): MD
}
