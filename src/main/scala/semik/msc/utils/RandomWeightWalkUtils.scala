package semik.msc.utils

import org.apache.spark.graphx.{Graph, PartitionStrategy}
import semik.msc.random.RWWVertexState
import semik.msc.random.struct.{RandomWeightWalkNeighbour, RandomWeightWalkVertex}

import scala.util.Random

/**
  * Created by mth on 2/7/17.
  */
object RandomWeightWalkUtils {

  def prepareGraph[VD, ED](graph: Graph[VD, ED]) =
    graph.partitionBy(PartitionStrategy.EdgePartition2D).ops.removeSelfEdges().ops.convertToCanonicalEdges((_, a) => a)

  def initializeVertices[VD, ED](graph: Graph[VD, ED]): Graph[RandomWeightWalkVertex, ED] = {
    val rho = (graph.ops.degrees.map(_._2).max() * 1.3).ceil
    val degrees = GraphUtils.collectDegrees(graph)((_, _, d) => d.getOrElse(0))

    GraphUtils.collectNeighboursIds(degrees)((vertexId, degree, neighbours) => {
      val neighboursArray = neighbours.getOrElse(Array())
      val likelihoodTuples = neighboursArray.map(neighbourId => RandomWeightWalkNeighbour(neighbourId, 1 / rho))
      val vertexWeight = 1 - degree / rho
      RandomWeightWalkVertex(vertexWeight, likelihoodTuples)
    })
  }

  def  selectNeighbours[ED](graph: Graph[RandomWeightWalkVertex, ED], quantum: Double) = {
    graph.mapVertices((id, v) => {
      if (v.vertexWeight < quantum) v
      else {
        val activeNbs = v.neighbours.filter(nb => nb.vertexState != RWWVertexState.completed)
        if (activeNbs.isEmpty) v
        else{
          val selected = activeNbs(Random.nextInt(activeNbs.length))
          RandomWeightWalkVertex(v.vertexWeight, v.neighbours, Some(selected.vertexId))
        }
      }
    })
  }

  def neighbourIndexMap(vertex: RandomWeightWalkVertex) =
    vertex.neighbours.map(_.vertexId).zipWithIndex.toMap
}
