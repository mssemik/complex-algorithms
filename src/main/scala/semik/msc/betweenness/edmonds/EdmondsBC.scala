package semik.msc.betweenness.edmonds

import org.apache.spark.graphx._
import semik.msc.betweenness.edmonds.predicate.EdmondsBCPredicate
import semik.msc.betweenness.edmonds.processor.EdmondsBCProcessor
import semik.msc.betweenness.edmonds.struct.EdmondsVertex
import semik.msc.betweenness.normalizer.BCNormalizer
import semik.msc.bfs.BFSShortestPath
import semik.msc.util.GraphSimplifier

/**
  * Created by mth on 3/12/17.
  */

class EdmondsBC[VD, ED](graph: Graph[VD, ED]) extends Serializable {

  lazy val simpleGraph = prepareRawGraph.cache

  val normalizer = new BCNormalizer(graph)
  val bcAggregator = new EdmondsBCAggregator[ED]
  lazy val edmondsBFSProcessor = new BFSShortestPath[EdmondsVertex, ED, (List[VertexId], Int, Int)](new EdmondsBCPredicate, new EdmondsBCProcessor)

  private def prepareRawGraph = {
    val simpleGraph = GraphSimplifier.simplifyGraph(graph)((m, _) => m)
    simpleGraph.mapVertices((vId, attr) => EdmondsVertex())
  }

  def computeBC = {

    var betweennessVector: Option[VertexRDD[Double]] = None

    val verticesIds = simpleGraph.vertices.map({ case (vertexId, _) => vertexId }).cache
    val verticesIterator = verticesIds.toLocalIterator

    while (verticesIterator.hasNext) {
      val processedVertex = verticesIterator.next

      val bfs = edmondsBFSProcessor.computeSingleSelectedSourceBFS(simpleGraph, processedVertex)
      val computedGraph = bcAggregator.aggregate(bfs, processedVertex)

      val partialBetweennessVector = computedGraph.vertices.mapValues(_.bc)

      val previousBetweennessVector = betweennessVector
      betweennessVector = updateBC(betweennessVector, partialBetweennessVector)

      betweennessVector match { case Some(vector) => vector.localCheckpoint; vector.count }
      previousBetweennessVector match { case Some(vector) => vector.unpersist(false); case None => }

      bfs.unpersist(false)
      computedGraph.unpersistVertices(false)
      computedGraph.edges.unpersist(false)
    }

    verticesIds.unpersist(false)
    finalize(betweennessVector)
  }

  private def updateBC(bcVector: Option[VertexRDD[Double]], partialBc: VertexRDD[Double]) =
    bcVector match {
      case Some(v) =>
        val bcVector = v.innerJoin(partialBc)((vId, bc1, bc2) => bc1 + bc2)
        Some(bcVector)
      case None => Some(partialBc)
    }

  private def finalize(bcVector: Option[VertexRDD[Double]]) = {
    val bc = bcVector.get
    val result = normalizer.normalize(bc).cache
    result.count
    bc.unpersist(false)
    result
  }
}
