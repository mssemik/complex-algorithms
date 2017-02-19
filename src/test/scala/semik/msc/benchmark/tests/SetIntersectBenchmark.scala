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

  val rand: Random = new Random()

  def fillSet1 = {
    val range = sizeOfSets * 3
    while (set1.size < sizeOfSets)
      set1 = set1 + rand.nextInt(range)
  }

  def fillSet2 = {
    val range = sizeOfSets * 3
    while (set2.size < sizeOfSets)
      set2 = set2 + rand.nextInt(range)
  }

  def fillIntersectSet = {
    val range = sizeOfSets * 3
    while (intersectSet.size < sizeOfSets)
      intersectSet = intersectSet + rand.nextInt(range)
  }

  override def preExecute: Unit = {
    fillSet1
    fillSet2
    fillIntersectSet
  }

  override def execute(round: Int): Unit = {
    f(set1, set2, intersectSet)
  }

  override def postExecute: Unit = {
    set1 = Set()
    set2 = Set()
    intersectSet = Set()
  }
}
