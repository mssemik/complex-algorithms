package semik.msc.betweenness.flow.processor

import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import semik.msc.betweenness.flow.generator.FlowGenerator
import semik.msc.betweenness.flow.struct.{CFBCFlow, CFBCVertex}
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

  def joinReceivedFlows(vertexId: VertexId, vertex: CFBCVertex, msg: List[CFBCFlow]) =
    vertex.applyNeighbourFlows(msg.toArray)


  def applyFlows(epsilon: Double)(id: VertexId, data: CFBCVertex) = {
    val keyExtF = (f: CFBCFlow) => (f.src, f.dst)

    def updateFlow(contextFlow: CFBCFlow, otherFlows: List[CFBCFlow]) = {
      val newPotential = (otherFlows.map(_.potential).sum + contextFlow.supplyValue(id)) / data.degree
      val completed = Math.abs(contextFlow.potential - newPotential) < epsilon
      CFBCFlow(contextFlow.src, contextFlow.dst, newPotential, completed)
    }

    val flowsKeys = (data.vertexFlows.map(keyExtF) ++ data.neighboursFlows.map(keyExtF)) distinct

    val msgGroups = data.neighboursFlows.groupBy(keyExtF)

    val newFlows = for (key <- flowsKeys) yield {
      val currentFlows = msgGroups.getOrElse(key, Array.empty).toList
      data.flowsMap.get(key) match {
        case Some(flow) => /*if (flow.completed) Some(flow) else*/ Some(updateFlow(flow, currentFlows))
        case None =>
          val completed = currentFlows.map(_.completed).reduce(_ || _)
          if (completed) None else Some(updateFlow(CFBCFlow.empty(key._1, key._2), currentFlows))
      }
    }

    data.updateFlows(newFlows.filter(_.nonEmpty).map(_.get))
  }

  def computeBetweenness(vertexId: VertexId, vertex: CFBCVertex) = {
    val groupedFlows = vertex.neighboursFlows.groupBy(f => (f.src, f.dst))
    val applicableFlows = groupedFlows.filterKeys({ case (src, dst) => src != vertexId && dst != vertexId })
    val completedReceivedFlows = applicableFlows.filter({ case ((_, _), f) => f.map(_.completed).reduce(_ && _) })
    val completedFlows = completedReceivedFlows.filter({ case ((s, d), f) => vertex.getFlow((s, d)).completed })

    val currentFlows = completedFlows.map({ case ((s, d), fl) =>
      val contextFlow = vertex.getFlow((s, d))
      fl.map(f => Math.abs(f.potential - contextFlow.potential)).sum / 2
    })

    vertex.updateBC(currentFlows.toSeq)
  }

  def removeCompletedFlows(vertexId: VertexId, vertex: CFBCVertex) = {
    val groupedFlows = vertex.neighboursFlows.groupBy(f => (f.src, f.dst))
    val completedReceivedFlows = groupedFlows.filter({ case ((_, _), f) => f.map(_.completed).reduce(_ && _) }).keySet
    val completedFlows = vertex.vertexFlows.filter(f => f.completed && completedReceivedFlows.contains((f.src, f.dst)))

    vertex.removeFlows(completedFlows)
  }

  def extractFlowMessages(graph: Graph[CFBCVertex, ED]) =
    graph.aggregateMessages[Array[CFBCFlow]](ctx => {
      ctx.sendToDst(ctx.srcAttr.vertexFlows)
      ctx.sendToSrc(ctx.dstAttr.vertexFlows)
    }, _ ++ _)

  def preMessageExtraction(eps: Double)(graph: Graph[CFBCVertex, ED], msg: VertexRDD[Array[CFBCFlow]]) =
    graph.joinVertices(msg)((vertexId, vertex, vertexMsg) => {
      val newVert = vertex.applyNeighbourFlows(vertexMsg)
      applyFlows(eps)(vertexId, newVert)
    })

  def postMessageExtraction(graph: Graph[CFBCVertex, ED]) =
    graph.mapVertices((id, vertex) => {
      val vertWithBC = computeBetweenness(id, vertex)
      removeCompletedFlows(id, vertWithBC)
    })

}
