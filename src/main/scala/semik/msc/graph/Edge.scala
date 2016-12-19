package semik.msc.graph

/**
  * Created by mth on 12/18/16.
  */
class Edge(val vertId: Long, val weight: Double) extends Serializable {
  var attr: Map[String, Any] = Map()
}

object Edge {
  def apply(vertId: Long, weight: Double = 1.0D) = new Edge(vertId, weight)
}
