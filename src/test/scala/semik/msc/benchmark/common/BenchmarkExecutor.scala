package semik.msc.benchmark.common

/**
  * Created by mth on 1/7/17.
  */
object BenchmarkExecutor {
  def apply(name: String, benchmark: Benchmark) = new BenchmarkExecutor(name, benchmark).execute
}

class BenchmarkExecutor(name: String, benchmark: Benchmark) {

  var timeOfExecutions: List[Long] = List()

  def execute = {
    for (i <- (1 to benchmark.numberOfExecutions)) {
      benchmark.preExecute
      val startTime = System.nanoTime()
      benchmark.execute(i)
      val endTime = System.nanoTime()
      benchmark.postExecute

      val duration = endTime - startTime
      timeOfExecutions = duration :: timeOfExecutions
    }

    val maxTime = timeOfExecutions.max
    val minTime = timeOfExecutions.min
    val avgTime = timeOfExecutions.reduce((x, y) => x + y)/timeOfExecutions.size

    println(name + " -> Min time: " + minTime + ", Max time: " + maxTime + ", Avg time: " + avgTime)
  }
}
