package semik.msc.benchmark.tests

import semik.msc.benchmark.common.tests.SetIntersectBenchmark

/**
  * Created by mth on 1/7/17.
  */
class SetIntersectWithGrowingSetBenchmark(numberOfExecutions: Int, sizeOfSets: Int, f: (Set[Long], Set[Long], Set[Long]) => Set[Long]) extends SetIntersectBenchmark(numberOfExecutions, sizeOfSets, f) {

  override def fillSet1 = {
    val range = sizeOfSets * 3
    while (set1.size < 20)
      set1 = set1 + rand.nextInt(range)
  }

  override def fillSet2 = {
    val range = sizeOfSets * 3
    while (set2.size < 20)
      set2 = set2 + rand.nextInt(range)
  }
}
