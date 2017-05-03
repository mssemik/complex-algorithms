package semik.msc.betweenness.flow.processor

import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import semik.msc.betweenness.flow.generator.FlowGenerator
import semik.msc.betweenness.flow.struct.{CFBCFlow, CFBCNeighbourFlow, CFBCVertex}
import semik.msc.utils.GraphSimplifier

/**
  * Created by mth on 4/23/17.
  */
class CFBCProcessor[VD, ED](graph: Graph[VD, ED], flowGenerator: FlowGenerator[CFBCVertex, Option[CFBCFlow]]) extends Serializable {

  lazy val initGraph = prepareRawGraph

  lazy val numOfVertices = graph.ops.numVertices

  private def prepareRawGraph = {
    val simpleGraph = GraphSimplifier.simplifyGraph(graph)((m, _) => m)
    val degrees = simpleGraph.ops.degrees
    simpleGraph.outerJoinVertices(degrees)((id, _, deg) => CFBCVertex(id, deg.getOrElse(0)))
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
    vertex.applyNeighbourFlows(msg.groupBy(_.key).map(it => CFBCNeighbourFlow(it._2, vertex)))


  def applyFlows(epsilon: Double)(id: VertexId, data: CFBCVertex) = {

    def updateFlow(contextFlow: CFBCFlow, nbhFlow: CFBCNeighbourFlow) = {
      val newPotential = (nbhFlow.sumOfPotential + contextFlow.supplyValue(id)) / data.degree
      val potentialDiff = Math.abs(contextFlow.potential - newPotential)
      val completed = potentialDiff < epsilon
      CFBCFlow(contextFlow.src, contextFlow.dst, newPotential, completed)
    }

    val flowsKeys = (data.vertexFlows.map(_.key) ++ data.neighboursFlows.map(_.key)) distinct

    val newFlows = for (key <- flowsKeys) yield {
      data.neighboursFlows.find(_.key == key) match {
        case Some(currentFlow) =>
          data.flowsMap.get(key) match {
            case Some(flow) => /*if (flow.completed) Some(flow) else*/ Some(updateFlow(flow, currentFlow))
            case None => if (currentFlow.anyCompleted) None else Some(updateFlow(CFBCFlow.empty(key._1, key._2), currentFlow))
          }
        case None => data.flowsMap.get(key)
      }
    }

    data.updateFlows(newFlows.filter(_.nonEmpty).map(_.get))
  }

  def computeBetweenness(vertexId: VertexId, vertex: CFBCVertex) = {
    val applicableFlows = vertex.neighboursFlows.filter(nf => nf.src != vertexId && nf.dst != vertexId)
    val completedReceivedFlows = applicableFlows.filter(_.allCompleted)
    val completedFlows = completedReceivedFlows.filter(nf => vertex.getFlow(nf.key).completed)

    val currentFlows = completedFlows.map(_.sumOfDifferences / 2)

    vertex.updateBC(currentFlows.toSeq)
  }

  def removeCompletedFlows(vertexId: VertexId, vertex: CFBCVertex) = {
    val completedReceivedFlows = vertex.neighboursFlows.filter(_.allCompleted).map(_.key).toSet
    val completedFlows = vertex.vertexFlows.filter(f => f.completed && completedReceivedFlows.contains(f.key))

    vertex.removeFlows(completedFlows)
  }

  def extractFlowMessages(graph: Graph[CFBCVertex, ED]) =
    graph.aggregateMessages[Array[CFBCFlow]](ctx => {
      ctx.sendToDst(ctx.srcAttr.vertexFlows)
      ctx.sendToSrc(ctx.dstAttr.vertexFlows)
    }, _ ++ _)

  def preMessageExtraction(eps: Double)(graph: Graph[CFBCVertex, ED], msg: VertexRDD[Array[CFBCFlow]]) =
    graph.ops.joinVertices(msg)((vertexId, vertex, vertexMsg) => {
      val newVert = joinReceivedFlows(vertexId, vertex, vertexMsg)
      applyFlows(eps)(vertexId, newVert)
    })

  def postMessageExtraction(graph: Graph[CFBCVertex, ED]) =
    graph.mapVertices((id, vertex) => {
      val vertWithBC = computeBetweenness(id, vertex)
      removeCompletedFlows(id, vertWithBC)
    })

}
