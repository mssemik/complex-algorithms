package semik.msc.betweenness.flow

import org.apache.spark.graphx.Graph
import semik.msc.betweenness.flow.generator.FlowGenerator
import semik.msc.betweenness.flow.processor.CFBCProcessor
import semik.msc.betweenness.flow.struct.{CFBCFlow, CFBCVertex}
import semik.msc.random.ctrw.ContinuousTimeRandomWalk

/**
  * Created by mth on 4/23/17.
  */
class CurrentFlowBC[VD, ED](graph: Graph[VD, ED], flowGenerator: FlowGenerator[CFBCVertex, Option[CFBCFlow]]) extends Serializable {

  lazy val cfbcProcessor = new CFBCProcessor[VD, ED](graph, flowGenerator)

  def computeBC(phi: Int, epsilon: Double) = {
    val ctrw = new ContinuousTimeRandomWalk[CFBCVertex, ED](cfbcProcessor.initGraph)
    val randomVertices = ctrw.sampleVertices(Math.ceil(phi * 1.4) toInt)

    val initGraph = cfbcProcessor.initGraph.joinVertices(randomVertices)((id, v, m) => CFBCVertex(id, v.degree, v.bc, m.distinct.toArray))

    var g1 = cfbcProcessor.createFlow(initGraph).cache

    var msg = cfbcProcessor.extractFlowMessages(g1).cache

    var msgCount = msg.filter({ case (id, m) => m.nonEmpty }).count

    while (msgCount > 0) {
      val g2 = cfbcProcessor.preMessageExtraction(epsilon)(g1, msg).cache

      val oldMsg = msg
      msg = cfbcProcessor.extractFlowMessages(g2).cache
      msgCount = msg.filter({ case (id, m) => m.nonEmpty }).count

      val g3 = cfbcProcessor.postMessageExtraction(g2).cache

      g1.unpersist(false)

      g1 = cfbcProcessor.createFlow(g3).cache
      g1.vertices.count
      g1.edges.count

      g2.unpersist(false)
      g3.unpersist(false)
      oldMsg.unpersist(false)

    }

    g1
  }

}
