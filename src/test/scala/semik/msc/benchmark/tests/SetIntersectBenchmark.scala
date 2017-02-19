package semik.msc.benchmark.common.tests

import semik.msc.benchmark.common.Benchmark

import scala.util.Random

/**
  * Created by mth on 1/7/17.
  */
class SetIntersectBenchmark(val numberOfExecutions: Int, sizeOfSets: Int, f: (Set[Long], Set[Long], Set[Long]) => Set[Long]) extends Benchmark {

  var set1: Set[Long] = Set()
  var set2: Set[Long] = Set()
  var intersectSet: Set[Long] = Set()

  override def preExecute: Unit = {
    val rand = new Random()
    val range = sizeOfSets * 3

    for (i <- (1 to sizeOfSets)) {
      set1 = set1 + rand.nextInt(range)
      set2 = set2 + rand.nextInt(range)
      intersectSet = intersectSet + rand.nextInt(range)
    }
  }

  override def execute(round: Int): Unit = {
    f(set1, set2, intersectSet)
  }

  override def postExecute: Unit = {
    set1 = null
    set2 = null
    intersectSet = null
  }
}
