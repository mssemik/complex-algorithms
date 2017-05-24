package semik.msc.betweenness.flow.generator.impl

import semik.msc.betweenness.flow.generator.FlowGenerator
import semik.msc.betweenness.flow.struct.{CFBCFlow, CFBCVertex}
import semik.msc.factory.Factory

import scala.util.Random

/**
  * Created by mth on 4/28/17.
  */
class RandomFlowGenerator(phi: Int, n: Int, k: Int, factory: Factory[CFBCVertex, CFBCFlow]) extends FlowGenerator[CFBCVertex, Option[CFBCFlow]] {
  override def createFlow(arg: CFBCVertex): Option[CFBCFlow] =
    if (shouldGenerateFlow(arg)) Some(factory.create(arg)) else None


  def shouldGenerateFlow(vertex: CFBCVertex) = {
    val p = Math.max(0.0, (phi - vertex.vertexPhi) / n)
    val r = Random.nextDouble()
    r > p && vertex.availableSamples.nonEmpty
  }
}
