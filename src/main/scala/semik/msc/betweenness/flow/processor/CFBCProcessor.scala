package semik.msc.betweenness.flow.processor

import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId, VertexRDD}
import semik.msc.betweenness.flow.factory.FlowFactory
import semik.msc.betweenness.flow.struct.{CFBCEmptyFlow, CFBCFlow, CFBCVertex}
import semik.msc.utils.GraphSimplifier

import scala.util.Random

/**
  * Created by mth on 4/23/17.
  */
class CFBCProcessor[VD, ED](graph: Graph[VD, ED], flowFactory: FlowFactory) extends Serializable {

  lazy val initGraph = prepareRawGraph

  lazy val numOfVertices = graph.ops.numVertices

  private def prepareRawGraph = {
    val simpleGraph = GraphSimplifier.simplifyGraph(graph)((m, _) => m)
    val degrees = simpleGraph.ops.degrees
    simpleGraph.outerJoinVertices(degrees)((id, _, deg) => CFBCVertex(id, deg.getOrElse(0)))
  }

  def generateFlow(vertex: CFBCVertex, phi: Int): Option[CFBCFlow] = {
    val probability = Math.max(0, (phi - vertex.vertexPhi) / numOfVertices)
    val randomVal = Random.nextDouble()

    if (randomVal < probability && vertex.availableSamples.nonEmpty) Some(flowFactory.create(vertex)) else None
  }

  def createFlow(graph: Graph[CFBCVertex, ED], phi: Int) = {
    graph.mapVertices((id, data) =>
      generateFlow(data, phi) match {
        case Some(flow) => CFBCVertex(data.id, data.degree, data.bc, data.sampleVertices, flow +: data.flows, flow.dst +: data.correlatedVertices)
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



  def updateBC(graph: Graph[CFBCVertex, ED], flows: VertexRDD[List[CFBCFlow]]) = {
    graph.joinVertices(flows)((id, vertex, fls) => {
//      fls.groupBy(f => (f.src, f.dst))
    })
  }

  def updateFlows(epsilon: Double)(id: VertexId, data: CFBCVertex, msg: List[CFBCFlow]) = {

    def u(src: VertexId, dst: VertexId) =
      if (src == id) 1
      else if (dst == id) -1
      else 0

    val newFlows = for (((s, d), v) <- msg.groupBy(f => (f.src, f.dst))) yield {
        val newPotential = (v.aggregate(0d)((acc, f) => acc + f.potential, _ + _) + u(s, d)) / data.degree
        val currPotential = data.flows.find(f => f.src == s && f.dst == d)
        val completed = Math.abs(currPotential.getOrElse(CFBCEmptyFlow()).potential - newPotential) < epsilon
        CFBCFlow(s, d, newPotential, completed)
      }

    CFBCVertex(data.id, data.degree, data.bc, data.sampleVertices, newFlows.toArray, data.correlatedVertices)
  }

}
