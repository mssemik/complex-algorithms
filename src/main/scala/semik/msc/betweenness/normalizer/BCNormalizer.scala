package semik.msc.betweenness.normalizer

import org.apache.spark.graphx.{Graph, VertexRDD}

/**
  * Created by mth on 4/11/17.
  */
class BCNormalizer(graph: Graph[_, _], undirected: Boolean = true) extends Serializable {

  val n = graph.numVertices

  val denominator = if (undirected) ((n - 1) * (n - 2)) / 2 else (n - 1) * (n - 2)

  def normalize(bc: VertexRDD[Double]) = bc.mapValues(_ / denominator)

}
