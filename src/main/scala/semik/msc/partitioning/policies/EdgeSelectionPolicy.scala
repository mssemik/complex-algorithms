package semik.msc.partitioning.policies

import org.apache.spark.graphx.Edge
import semik.msc.utils.JaBeJaUtils._

/**
  * Created by mth on 1/29/17.
  */
object EdgeSelectionPolicy {
  val greedy = new GreedyEdgeSelection
}

trait EdgeSelectionPolicy[T] extends Serializable {
  def selectEdge(edges: Seq[Edge[T]], color: Option[Int] = None): Edge[T]
}

class GreedyEdgeSelection extends EdgeSelectionPolicy[Int] {
  def selectEdge(edges: Seq[Edge[Int]], color: Option[Int] = None): Edge[Int] =
    color match {
      case Some(c)  => getRandomElem(edges.filter(_.attr == c)).get
      case _        => selectEdge(edges, Some(edges.groupBy(_.attr).minBy(_._2.size)._1))
    }

}


