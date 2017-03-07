package semik.msc.neighbourhood

import org.apache.spark.graphx.{EdgeDirection, Graph}

/**
  * Created by mth on 3/6/17.
  */
class VertexNeighbourhood(graph: Graph[_, _]) extends Serializable {
  private lazy val opsGraph = graph.ops

  lazy val nearestNeighbourhood = opsGraph.collectNeighborIds(EdgeDirection.Either)
}

object VertexNeighbourhood extends Serializable {
  def apply(graph: Graph[_, _]): VertexNeighbourhood = new VertexNeighbourhood(graph)
}
