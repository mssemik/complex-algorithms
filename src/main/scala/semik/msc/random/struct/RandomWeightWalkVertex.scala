package semik.msc.random.struct

import org.apache.spark.graphx.VertexId
import semik.msc.random.{MessageType, RandomWeightWalkVertexState, VertexState}

/**
  * Created by mth on 2/7/17.
  */
class RandomWeightWalkVertex(
    val vertexWeight: Double,
    val neighbourMap: Map[VertexId, Double],
    val vertexState: VertexState = RandomWeightWalkVertexState.ready)
  extends Serializable {

  def updateWeight(newWeight: Double) = new RandomWeightWalkVertex(newWeight, neighbourMap, vertexState)

  def updateNeighbourMap(map: Map[VertexId, Double]) = new RandomWeightWalkVertex(vertexWeight, map, vertexState)

  def updateVertexState(newState: VertexState) = new RandomWeightWalkVertex(vertexWeight, neighbourMap, newState)

}
