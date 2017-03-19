package semik.msc.bfs.predicate

import org.apache.spark.graphx.VertexId
import semik.msc.predicate.vertex.VertexPredicate

/**
  * Created by mth on 3/15/17.
  */
class BFSVertexPredicate extends VertexPredicate[List[VertexId], List[VertexId]]{
  override def getInitialData(vertexId: VertexId, attr: List[VertexId]): (VertexId) => List[VertexId] = (v) =>
    if (v == vertexId) List(v) else List.empty

  override def applyMessages(vertexId: VertexId, data: List[VertexId], message: List[VertexId]): List[VertexId] =
    if (data.isEmpty) message else data

}
