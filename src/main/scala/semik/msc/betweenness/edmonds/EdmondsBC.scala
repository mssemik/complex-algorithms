package semik.msc.betweenness.edmonds

import org.apache.spark.graphx._
import semik.msc.betweenness.edmonds.predicate.EdmondsBCPredicate
import semik.msc.betweenness.edmonds.processor.EdmondsBCProcessor
import semik.msc.betweenness.edmonds.struct.{EdmondsSPVertex, EdmondsVertex}
import semik.msc.bfs.BFSShortestPath
import semik.msc.util.GraphSimplifier

/**
  * Created by mth on 3/12/17.
  */

class EdmondsBC[VD, ED](graph: Graph[VD, ED]) extends Serializable {

  lazy val simpleGraph = prepareGraph.cache()

  private def prepareGraph = {
    val simpleGraph = GraphSimplifier.simplifyGraph(graph)((m, _) => m)
    simpleGraph.mapVertices((vId, attr) => EdmondsVertex())
  }

  def run = {

    val verticesIds = simpleGraph.vertices.map({ case (vertexId, _) => vertexId}).cache()
    val verticesIterator = verticesIds.toLocalIterator

    val ed = new BFSShortestPath[EdmondsVertex, ED, (List[VertexId], Int, Int)](new EdmondsBCPredicate, new EdmondsBCProcessor)

    var tempGraph = simpleGraph
    var oldGraph = simpleGraph
    var prevBFS: Graph[EdmondsVertex, ED] = null

    if (verticesIterator.hasNext) {
      val v = verticesIterator.next()
      val bfs = ed.computeSingleSelectedSourceBFS(tempGraph, v).cache()
      tempGraph = updateCentrality(bfs, v).cache()

      prevBFS = bfs
    }

    while (verticesIterator.hasNext) {
      val vertex = verticesIterator.next()
      val bfs = ed.computeSingleSelectedSourceBFS(tempGraph, vertex).cache()

      tempGraph = updateCentrality(bfs, vertex).cache()
      println("Obsłużony wierzchołek: " + vertex)

      prevBFS.unpersist(false)
      oldGraph.unpersist(false)
      oldGraph = tempGraph
      prevBFS = bfs
    }

    verticesIds.unpersist()
    tempGraph
  }

  private def updateCentrality(graph: Graph[EdmondsVertex, ED], source: VertexId) = {
    def aggregateMessages(graph: Graph[EdmondsVertex, ED], depth: Int) = graph.aggregateMessages[Double](
      edgeContext => {
        val sender = createAndSendMessage(edgeContext.toEdgeTriplet, depth) _
        sender(edgeContext.srcId, edgeContext.sendToDst)
        sender(edgeContext.dstId, edgeContext.sendToSrc)
      }, Math.max
    )

    def createAndSendMessage(triplet: EdgeTriplet[EdmondsVertex, ED], depth: Int)(source: VertexId, f: (Double) => Unit) = {
      val attr = triplet.vertexAttr(source)
      if (attr.depth == depth) sendMessage(produceMessage(triplet)(source), f)
    }

    def produceMessage(triplet: EdgeTriplet[EdmondsVertex, ED])(source: VertexId) = {
      val attr = triplet.vertexAttr(source)
      val otherAttr = triplet.otherVertexAttr(source)
      val delta = otherAttr.sigma / attr.sigma * (1 + attr.delta)
      if (attr.preds.contains(triplet.otherVertexId(source))) Some(delta) else None
    }

    def sendMessage(message: Option[Double], f: (Double) => Unit) = message.foreach(f)

    def applyMessages(graph: Graph[EdmondsVertex, ED], messages: VertexRDD[Double]) =
      graph.ops.joinVertices(messages)((vertexId, attr, message) => {
        val bc = if (vertexId != source) attr.bc + message else attr.bc
        EdmondsVertex(attr.preds, attr.sigma, attr.depth, message, bc)
      })

    val startTime = System.nanoTime()

    val maxDepth = graph.vertices.aggregate(0)((depth, v) => Math.max(v._2.depth, depth), Math.max)

    var g = graph.cache()
    var oldGraph = g
    var oldMessages: VertexRDD[Double] = null

    val messages = aggregateMessages(g, maxDepth).cache()
    val nom = messages.count()
    g = applyMessages(g, messages).cache()
    println("# of messages " + nom)
    oldGraph = g
    oldMessages = messages

    for (i <- 0 until maxDepth reverse) {
      val messages = aggregateMessages(g, i).cache()
      val nom = messages.count()
      g = applyMessages(g, messages).cache()
      println("# of messages " + nom)

      oldMessages.unpersist(false)
      oldGraph.unpersistVertices(false)
      oldGraph.edges.unpersist(false)
      oldGraph = g
      oldMessages = messages
    }

    val finishTime = System.nanoTime()
    println("Time of execution updateCentrality:" + ((finishTime - startTime)/1000000) + " ms")

    g
  }
}
