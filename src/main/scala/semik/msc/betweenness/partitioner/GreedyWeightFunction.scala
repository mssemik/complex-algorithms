package semik.msc.betweenness.partitioner

/**
  * Created by mth on 12/20/16.
  */
trait GreedyWeightFunction extends (((Int) => Int, Int, Int) => Double) with Serializable

object UnweightedGreedyFunction extends GreedyWeightFunction {
  def apply(f: (Int) => Int, c: Int, i: Int) = 1.0D
}

object LinearGreedyFunction extends GreedyWeightFunction {
  def apply(f: (Int) => Int, c: Int, i: Int) = 1.0D - f(i) / c
}

object ExponentialGreedyFunction extends GreedyWeightFunction {
  def apply(f: (Int) => Int, c: Int, i: Int) = 1.0D - Math.pow(Math.E, f(i) - c)
}
