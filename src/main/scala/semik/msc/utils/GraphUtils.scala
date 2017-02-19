package semik.msc.utils

import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId}

import scala.reflect.ClassTag

/**
  * Created by mth on 2/6/17.
  */
object GraphUtils {

  def collectDegrees[VD, ED, VD2: ClassTag](graph: Graph[VD, ED])(mapFunc: ((VertexId, VD, Option[Int]) => VD2)) =
    graph.outerJoinVertices(graph.ops.degrees)(mapFunc)

  def collectNeighboursIds[VD, ED, VD2: ClassTag](graph: Graph[VD, ED])(mapFunc: ((VertexId, VD, Option[Array[VertexId]]) => VD2)) =
    graph.outerJoinVertices(graph.ops.collectNeighborIds(EdgeDirection.Either))(mapFunc)

}
