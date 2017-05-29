package semik.msc.betweenness.flow.processor

import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId, VertexRDD}
import org.apache.spark.storage.StorageLevel
import semik.msc.betweenness.flow.generator.FlowGenerator
import semik.msc.betweenness.flow.struct.{CFBCFlow, CFBCNeighbourFlow, CFBCVertex}
import semik.msc.utils.GraphSimplifier

import scala.reflect.ClassTag

/**
  * Created by mth on 4/23/17.
  */
class CFBCProcessor[VD, ED: ClassTag](graph: Graph[VD, ED], flowGenerator: FlowGenerator[CFBCVertex, Option[CFBCFlow]]) extends Serializable {

  lazy val initGraph = prepareRawGraph

  lazy val numOfVertices = graph.ops.numVertices

  private def prepareRawGraph = {
    val simpleGraph = GraphSimplifier.simplifyGraph(graph)((m, _) => m)
    val degrees = simpleGraph.ops.degrees
    val temp = simpleGraph.outerJoinVertices(degrees)((id, _, deg) => CFBCVertex(id, deg.getOrElse(0)))
    Graph[CFBCVertex, ED](
      vertices = temp.vertices,
      edges = temp.edges,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK,
      edgeStorageLevel = StorageLevel.MEMORY_AND_DISK
    )
  }

  def createFlow(graph: Graph[CFBCVertex, ED]) = {
    graph.mapVertices((id, data) =>
      flowGenerator.createFlow(data) match {
        case Some(flow) => data.addNewFlow(flow)
        case None => data
      })

    //    val msg1 = VertexRDD(newGrapg.vertices.flatMap({ case (id, d) => d.flows.filter(_.potential == 1.0d)
    //      .map(c => (c.dst, id))}).aggregateByKey(List.empty[VertexId])((l, t) => t +: l, _ ++ _))
    //
    //    val result = newGrapg.outerJoinVertices(msg1)((id, data, msgs) =>
    //      msgs match {
    //        case Some(list) =>
    //          val conflicted = data.correlatedVertices.intersect(list)
    //          val flowsToRemove = conflicted.filter(_ > id)
    //          val correctedFlows = data.flows.filterNot(f => flowsToRemove.contains(f.dst) && f.src == id && f.potential == 1.0d)
    //          val correlatedVertices = (data.correlatedVertices ++ list).distinct
    //          CFBCVertex(id, data.sampleVertices, correctedFlows, correlatedVertices)
    //        case None => data
    //      }).cache()
    //
    //    result.vertices.count
    //    result.edges.count
    //
    //    newGrapg.unpersist(false)
    //    result
  }

  def joinReceivedFlows(vertexId: VertexId, vertex: CFBCVertex, msg: Array[CFBCFlow]) =
    vertex.applyNeighbourFlows(msg.groupBy(_.key).map(it => if (it._2.nonEmpty) CFBCNeighbourFlow(it._2, vertex) else CFBCNeighbourFlow(it._1._1, it._1._2)))


  def applyFlows(epsilon: Double)(id: VertexId, data: CFBCVertex) = {

    def updateFlow(contextFlow: CFBCFlow, nbhFlow: CFBCNeighbourFlow) = {
      val newPotential = (nbhFlow.sumOfPotential + contextFlow.supplyValue(id)) / data.degree
      val potentialDiff = Math.abs(contextFlow.potential - newPotential)
      val completed = contextFlow.completed || potentialDiff < epsilon
      CFBCFlow(contextFlow.src, contextFlow.dst, newPotential, completed)
    }

    val newFlows = for (nb <- data.neighboursFlows) yield {
      val flowOpt = data.flowsMap.get(nb.key)
      flowOpt match {
        case Some(flow) if flow.completed && nb.allCompleted => Some(flow)
        case Some(flow) => Some(updateFlow(flow, nb))
        case None if !nb.anyCompleted => Some(updateFlow(CFBCFlow.empty(nb.key._1, nb.key._2), nb))
        case _ => None
      }
    }

    val k1 = data.vertexFlows.map(f => (f.key, f)).toMap
    val k2 = newFlows.filter(_.nonEmpty).map(f => (f.get.key, f.get)).toMap

    val k3 = k1.filterKeys(k => !k2.contains(k))
    val kk = k3 ++ k2

    data.updateFlows(kk.values.toArray)
  }

  def computeBetweenness(vertexId: VertexId, vertex: CFBCVertex) = {
    val applicableFlows = vertex.neighboursFlows.filter(nf => nf.src != vertexId && nf.dst != vertexId)
    val completedReceivedFlows = applicableFlows.filter(_.allCompleted)
    val completedFlows = completedReceivedFlows.filter(nf => vertex.getFlow(nf.key).completed)

    val currentFlows = completedFlows.map(_.sumOfDifferences / 2)

    vertex.updateBC(currentFlows.toSeq)
  }

  def removeCompletedFlows(vertexId: VertexId, vertex: CFBCVertex) = {
    val completedReceivedFlows = vertex.neighboursFlows.map(nf => (nf.key, nf.allCompleted)).toMap
    val completedFlows = vertex.vertexFlows.filter(f => f.removable && completedReceivedFlows.getOrElse(f.key, true))

    vertex.removeFlows(completedFlows)
  }

  def extractFlowMessages(graph: Graph[CFBCVertex, ED]) =
    graph.aggregateMessages[Array[CFBCFlow]](ctx => {

      def send(triplet: EdgeTriplet[CFBCVertex, ED])(dst: VertexId, sendF: (Array[CFBCFlow]) => Unit): Unit = {
        val srcFlows = triplet.otherVertexAttr(dst).vertexFlows
        val dstFlowsKeys = triplet.vertexAttr(dst).vertexFlows.map(_.key).toSet
        val activeFlows = srcFlows.filterNot(_.completed)
        val completedFlows = srcFlows.filter(f => f.completed && dstFlowsKeys.contains(f.key))
        sendF(activeFlows ++ completedFlows)
      }

      val sendDataTo = send(ctx.toEdgeTriplet) _
      sendDataTo(ctx.srcId, ctx.sendToSrc)
      sendDataTo(ctx.dstId, ctx.sendToDst)
    }, _ ++ _)

  def preMessageExtraction(eps: Double)(graph: Graph[CFBCVertex, ED], msg: VertexRDD[Array[CFBCFlow]]) =
    graph.outerJoinVertices(msg)((vertexId, vertex, vertexMsg) => {
      val newVert = joinReceivedFlows(vertexId, vertex, vertexMsg.getOrElse(Array.empty))
      applyFlows(eps)(vertexId, newVert)
    })

  def postMessageExtraction(graph: Graph[CFBCVertex, ED]) =
    graph.mapVertices((id, vertex) => {
      val vertWithBC = computeBetweenness(id, vertex)
      removeCompletedFlows(id, vertWithBC)
    })

}
