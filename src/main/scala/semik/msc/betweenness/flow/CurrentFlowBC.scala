package semik.msc.betweenness.flow

import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import semik.msc.betweenness.flow.factory.FlowFactory
import semik.msc.betweenness.flow.processor.CFBCProcessor
import semik.msc.betweenness.flow.struct.{CFBCFlow, CFBCVertex}
import semik.msc.pregel.Pregel
import semik.msc.random.ctrw.ContinuousTimeRandomWalk

import scala.util.Random

/**
  * Created by mth on 4/23/17.
  */
class CurrentFlowBC[VD, ED](graph: Graph[VD, ED]) extends Serializable {

  lazy val cfbcProcessor = new CFBCProcessor[VD, ED](graph, new FlowFactory)

  def computeBC(phi: Int, epsilon: Double) = {
    val ctrw = new ContinuousTimeRandomWalk[CFBCVertex, ED](cfbcProcessor.initGraph)
    val randomVertices = ctrw.sampleVertices(Math.ceil(phi * 1.4) toInt)

    val initGraph = cfbcProcessor.initGraph.joinVertices(randomVertices)((id, v, m) => CFBCVertex(id, v.degree, v.bc, m.distinct.toArray))


    val g1 = cfbcProcessor.createFlow(initGraph, phi)



  }

}
