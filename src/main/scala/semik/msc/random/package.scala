package semik.msc

import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 2/7/17.
  */
package object random {
  type VertexState = Int
  type MessageType = Int
  type Neighbour = (VertexId, Double, VertexState)
  type Vertex = (Double, Array[Neighbour], Option[List[Boolean]])
}
