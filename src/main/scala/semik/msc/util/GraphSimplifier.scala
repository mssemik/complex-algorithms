package semik.msc.util

import org.apache.spark.graphx.Graph

/**
  * Created by mth on 3/18/17.
  */
object GraphSimplifier extends Serializable {

  def removeSelfEdges[VD, ED](graph: Graph[VD, ED]) =
    graph.ops.removeSelfEdges()

  def convertToCanonicalEdges[VD,ED](graph: Graph[VD, ED])(m: (ED, ED) => ED = (m1: ED, m2: ED) => m1) =
    graph.ops.convertToCanonicalEdges(m)

  def simplifyGraph[VD, ED](graph: Graph[VD, ED])(m: (ED, ED) => ED = (m1: ED, m2: ED) => m1) =
    graph.ops.removeSelfEdges().ops.convertToCanonicalEdges(m)
}
