package semik.msc.betweenness.edmonds

import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import semik.msc.betweenness.edmonds.predicate.EdmondsBCPredicate
import semik.msc.betweenness.edmonds.processor.EdmondsBCProcessor
import semik.msc.betweenness.edmonds.struct.EdmondsVertex
import semik.msc.betweenness.normalizer.BCNormalizer
import semik.msc.bfs.BFSShortestPath
import semik.msc.utils.GraphSimplifier

import scala.reflect.ClassTag

/**
  * Created by mth on 3/12/17.
  */

class EdmondsBC[VD, ED: ClassTag](graph: Graph[VD, ED]) extends Serializable {

  lazy val simpleGraph = prepareRawGraph

  val normalizer = new BCNormalizer(graph)
  val bcAggregator = new EdmondsBCAggregator[ED]
  lazy val edmondsBFSProcessor = new BFSShortestPath[EdmondsVertex, ED, (List[VertexId], Int, Int)](new EdmondsBCPredicate, new EdmondsBCProcessor)

  private def prepareRawGraph = {
    graph.mapVertices((vId, attr) => EdmondsVertex())
  }

  def computeBC = {

    val verticesIds = simpleGraph.vertices.map({ case (vertexId, _) => vertexId }).cache
    val verticesIterator = verticesIds.toLocalIterator
    var betweennessVector: VertexRDD[Double] = simpleGraph.vertices.mapValues(_ => .0).cache()
    var i = 0

    while (verticesIterator.hasNext) {
      val processedVertex = verticesIterator.next

      val bfs = edmondsBFSProcessor.computeSingleSelectedSourceBFS(simpleGraph, processedVertex)
      val computedGraph = bcAggregator.aggregate(bfs, processedVertex)

      val partialBetweennessVector = computedGraph.vertices.mapValues(_.bc)

      val previousBetweennessVector = betweennessVector
      betweennessVector = updateBC(betweennessVector, partialBetweennessVector)

      betweennessVector.checkpoint()
      betweennessVector.count
      previousBetweennessVector.unpersist(false)

      bfs.unpersist(false)
      computedGraph.unpersistVertices(false)
      computedGraph.edges.unpersist(false)

      if (i % 10 == 0) println(s"Edm -> $i")
      i = i + 1
    }

    verticesIds.unpersist(false)
    finalize(betweennessVector)
  }

  private def updateBC(bcVector: VertexRDD[Double], partialBc: VertexRDD[Double]) =
    bcVector.innerJoin(partialBc)((vId, bc1, bc2) => bc1 + bc2)

  private def finalize(bcVector: VertexRDD[Double]) = {
    val result = normalizer.normalize(bcVector.mapValues(_ / 2)).cache
    result.count
    bcVector.unpersist(false)
    result
  }
}
