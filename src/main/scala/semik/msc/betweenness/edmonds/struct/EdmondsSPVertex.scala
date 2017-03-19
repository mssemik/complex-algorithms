package semik.msc.betweenness.edmonds.struct

import org.apache.spark.graphx._

/**
  * Created by mth on 3/12/17.
  */
class EdmondsSPVertex(
                     val currSrc: VertexId,
                     val parent: Option[VertexId],
                     val successors: List[VertexId])
  extends Serializable

object EdmondsSPVertex {

  def apply(
             currSrc: VertexId,
             parent: Option[VertexId] = None,
             successors: List[VertexId] = List.empty): EdmondsSPVertex =
    new EdmondsSPVertex(currSrc, parent, successors)

  def unapply(arg: EdmondsSPVertex): Option[(VertexId, Option[VertexId], List[VertexId])] =
    Some((arg.currSrc, arg.parent, arg.successors))
}
