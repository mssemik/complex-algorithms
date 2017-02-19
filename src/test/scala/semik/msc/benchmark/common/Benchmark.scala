package semik.msc.benchmark.common

/**
  * Created by mth on 1/7/17.
  */
trait Benchmark {
  val numberOfExecutions: Int
  def preExecute
  def execute(round: Int)
  def postExecute
}
