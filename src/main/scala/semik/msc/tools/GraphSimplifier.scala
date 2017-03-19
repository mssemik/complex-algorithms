package semik.msc.tools

import org.apache.spark.graphx.Graph

/**
  * Created by mth on 3/8/17.
  */
class GraphSimplifier[VD, ED](graph: Graph[VD, ED]) extends Serializable {

  lazy val simpleNaiveGraph = simplifyGraph()

  def simplifyGraph(mergeEdges: (ED, ED) => ED = (e1, e2) => e1) = {
    val g1 = graph.ops.convertToCanonicalEdges()
    val g2 = g1.ops.removeSelfEdges()
    val g3 = g2.groupEdges(mergeEdges)
    g2.unpersist(false)
    g3
  }

}