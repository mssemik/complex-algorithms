package semik.msc.random.struct

import org.apache.spark.graphx.VertexId
import semik.msc.random.{RWWVertexState, VertexState}

/**
  * Created by mth on 2/7/17.
  */
class RandomWeightWalkNeighbour( val vertexId: VertexId,
                                 val weight: Double,
                                 val vertexState: VertexState) extends Serializable

object RandomWeightWalkNeighbour extends Serializable {

  def apply( vertexId: VertexId,
             weight: Double,
             vertexState: VertexState = RWWVertexState.ready): RandomWeightWalkNeighbour =
    new RandomWeightWalkNeighbour(vertexId, weight, vertexState)
}