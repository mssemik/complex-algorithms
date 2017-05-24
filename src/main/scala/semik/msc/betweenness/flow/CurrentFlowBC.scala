package semik.msc.betweenness.flow

import org.apache.spark.graphx._
import semik.msc.betweenness.flow.generator.FlowGenerator
import semik.msc.betweenness.flow.processor.CFBCProcessor
import semik.msc.betweenness.flow.struct.{CFBCFlow, CFBCVertex}
import semik.msc.random.ctrw.ContinuousTimeRandomWalk

import scala.util.Random

/**
  * Created by mth on 4/23/17.
  */
class CurrentFlowBC[VD, ED](graph: Graph[VD, ED], flowGenerator: FlowGenerator[CFBCVertex, Option[CFBCFlow]]) extends Serializable {

  lazy val cfbcProcessor = new CFBCProcessor[VD, ED](graph, flowGenerator)

  def computeBC(phi: Int, epsilon: Double) = {
    val ctrw = new ContinuousTimeRandomWalk[CFBCVertex, ED](cfbcProcessor.initGraph)
    val randomVertices = ctrw.sampleVertices(Math.ceil(phi * 1.6) toInt)

    val initGraph = cfbcProcessor.initGraph.ops.joinVertices(randomVertices)((id, v, m) => {
      val sample = Random.shuffle(m.distinct).take(phi)
      CFBCVertex(id, v.degree, v.bc, sample.toArray)

    })

    var i = 1

    var g1 = cfbcProcessor.createFlow(initGraph).mapVertices(cfbcProcessor.applyFlows(epsilon)).cache

    var msg = cfbcProcessor.extractFlowMessages(g1).cache

    var msgCount = msg.filter({ case (id, m) => m.nonEmpty }).count

//    println(s"NumOfVertices: ${getNumberOfFlows(g1)}, NumOfMsg: $msgCount")

    while (msgCount > 0) {
      val g2 = cfbcProcessor.preMessageExtraction(epsilon)(g1, msg).cache

      val oldMsg = msg
      msg = cfbcProcessor.extractFlowMessages(g2).cache
      msgCount = msg.filter({ case (id, m) => m.nonEmpty }).count

      val g3 = cfbcProcessor.postMessageExtraction(g2).cache

      if (i % 20 == 0) { g3.checkpoint(); g3.vertices.count(); g3.edges.count() }

      g1.unpersist(false)

      g1 = cfbcProcessor.createFlow(g3).cache
      g1.vertices.count
      g1.edges.count

      g2.unpersist(false)
      g3.unpersist(false)
      oldMsg.unpersist(false)

      println(s"NumOfFlowsToActivate: ${getNumberOfFlows(g1)} :: ${getNumberOfFetureFlows(g1)}, NumOfMsg: $msgCount")
      i = i + 1
    }

    initGraph.unpersist(false)

    g1.vertices
  }

  def getNumberOfFlows(g: Graph[CFBCVertex, _]) = g.vertices.map({ case (_, v) => v.vertexFlows.length }).reduce(_ + _)

  def getNumberOfFetureFlows(g: Graph[CFBCVertex, _]) = g.vertices.map({ case (_, v) => v.availableSamples.length }).reduce(_ + _)

}
