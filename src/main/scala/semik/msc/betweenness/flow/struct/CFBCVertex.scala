package semik.msc.betweenness.flow.struct

import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 4/23/17.
  */
class CFBCVertex(val id: VertexId, val degree: Int, val bc: Double, val sampleVertices: Array[VertexId], val flows: (Array[CFBCFlow], Array[CFBCFlow]), val processedFlows: Int) extends Serializable {
  lazy val relatedFlows = flows._1.filter(f => f.dst == id || f.src == id)
  lazy val availableSamples = sampleVertices.diff(flows._1.filter(_.src == id).map(_.dst))

  lazy val vertexPhi = flows._1.count(_.src == id)

  lazy val flowsMap = flows._1.map(f => ((f.src, f.dst), f)).toMap

  val vertexFlows = flows._1
  val neighboursFlows = flows._2

  def getFlow(key: (VertexId, VertexId)) = flowsMap.getOrElse(key, CFBCFlow.empty(key._1, key._2))

  def updateBC(currentFlowing: Double) = {
    val newBC = (processedFlows * bc + currentFlowing) / (processedFlows + 1)
    new CFBCVertex(id, degree, newBC, sampleVertices, flows, processedFlows + 1)
  }

  def updateBC(currentFlowing: Seq[Double]) = {
    val newBC = (processedFlows * bc + currentFlowing.sum) / (processedFlows + currentFlowing.length)
    new CFBCVertex(id, degree, newBC, sampleVertices, flows, processedFlows + currentFlowing.length)
  }

  def addNewFlow(flow: CFBCFlow) =
    new CFBCVertex(id, degree, bc, sampleVertices, (flows._1 :+ flow, flows._2), processedFlows)

  def updateFlows(fls: Array[CFBCFlow]) =
    new CFBCVertex(id, degree, bc, sampleVertices, (fls, flows._2), processedFlows)

  def removeFlows(toRemove: Seq[CFBCFlow]) =
    new CFBCVertex(id, degree, bc, sampleVertices, (flows._1.diff(toRemove), flows._2), processedFlows)

  def applyNeighbourFlows(nbhFlows: Array[CFBCFlow]) =
    new CFBCVertex(id, degree, bc, sampleVertices, (flows._1, nbhFlows), processedFlows)
}

object CFBCVertex extends Serializable {
  def apply(id: VertexId,
            degree: Int,
            bc: Double = 0.0,
            sampleVertices: Array[VertexId] = Array.empty,
            flows: (Array[CFBCFlow], Array[CFBCFlow]) = (Array.empty, Array.empty)
           ): CFBCVertex = new CFBCVertex(id, degree, bc, sampleVertices, flows, 0)
}
