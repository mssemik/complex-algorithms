package semik.msc.predicate.vertex

import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 3/15/17.
  */
trait VertexPredicate[VD, MD] extends Serializable {
  def getInitialData(vertexId: VertexId, attr: VD): (VertexId) => VD
  def applyMessages(vertexId: VertexId, date: VD, message: MD): VD
}
