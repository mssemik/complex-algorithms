package semik.msc.random.struct

import org.apache.spark.graphx.VertexId
import semik.msc.random.{MessageType, RWWVertexState, VertexState}

/**
  * Created by mth on 2/7/17.
  */
class RandomWeightWalkVertex( val vertexWeight: Double,
                              val neighbours: Array[RandomWeightWalkNeighbour],
                              val selected: Option[VertexId] = None,
                              val responses: Option[List[(VertexId, Boolean)]] = None) extends Serializable {

  private lazy val neighbourMap = neighbours.zipWithIndex.map(n => (n._1.vertexId, n._2)).toMap

  def updateNeighbour(nb: RandomWeightWalkNeighbour) = neighbours(neighbourMap.getOrElse(nb.vertexId, -1)) = nb

  def searchNeighbour(f: (RandomWeightWalkNeighbour) => Boolean) = neighbours.find(f)

  def neighbour(id: VertexId) = neighbours(neighbourMap.getOrElse(id, -1))
}

object RandomWeightWalkVertex extends Serializable {

  def apply( vertexWeight: Double,
             neighbours: Array[RandomWeightWalkNeighbour],
             selected: Option[VertexId] = None,
             responses: Option[List[(VertexId, Boolean)]] = None): RandomWeightWalkVertex =
    new RandomWeightWalkVertex(vertexWeight, neighbours, selected, responses)
}
