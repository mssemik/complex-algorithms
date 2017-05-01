package semik.msc.betweenness.flow.factory

import semik.msc.betweenness.flow.struct.{CFBCFlow, CFBCVertex}
import semik.msc.factory.Factory

import scala.util.Random

/**
  * Created by mth on 4/23/17.
  */
class FlowFactory extends Factory[CFBCVertex, CFBCFlow] {

  val initPotential = 1.0d

  override def create(arg: CFBCVertex): CFBCFlow = {
    val dst = arg.availableSamples(Random.nextInt(arg.availableSamples.length))
    CFBCFlow(arg.id, dst, initPotential / arg.degree)
  }

  override def correct(arg: CFBCVertex, curr: CFBCFlow): CFBCFlow = curr
}
