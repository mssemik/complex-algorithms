package semik.msc.betweenness.edmonds.predicate

import org.apache.spark.graphx.VertexId
import semik.msc.betweenness.edmonds.struct.EdmondsVertex
import semik.msc.predicate.vertex.VertexPredicate

/**
  * Created by mth on 3/15/17.
  */
class EdmondsBCPredicate extends VertexPredicate[EdmondsVertex, (List[VertexId], Int, Int)] {

  override def getInitialData(vertexId: VertexId, attr: EdmondsVertex): (VertexId) => EdmondsVertex =
    (vId) => if (vId == vertexId) EdmondsVertex(List(vId), 1, 0, 0, attr.bc) else EdmondsVertex(bc = attr.bc)

  override def applyMessages(vertexId: VertexId, date: EdmondsVertex, message: (List[VertexId], Int, Int)): EdmondsVertex =
    if (date.rooted) date else EdmondsVertex(message._1, message._2, message._3, 0, date.bc)

}
