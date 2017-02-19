package semik.msc.benchmark

import semik.msc.benchmark.common.BenchmarkExecutor
import semik.msc.benchmark.common.tests.SetIntersectBenchmark
import semik.msc.benchmark.tests.SetIntersectWithGrowingSetBenchmark

/**
  * Created by mth on 1/7/17.
  */
object TestExecutor {

  def main(args: Array[String]) = {
//    intersectAndJoin("intersectBeforeJoin", (x, y, z) => x.intersect(z) ++ y.intersect(z))
//    intersectAndJoin("intersectAfterJoin", (x, y, z) => (x ++ y).intersect(z))
//    intersectAndJoinWithGrowing("intersectBeforeJoinFirstSmallSet", (x, y, z) => x.intersect(z) ++ y.intersect(z))
    intersectAndJoinWithGrowing("intersectBeforeJoinFirstBigSet", (x, y, z) => z.intersect(x) ++ z.intersect(y))
  }

  def intersectAndJoin(name:String, f: (Set[Long], Set[Long], Set[Long]) => Set[Long]) = {

    println("Warming up...")
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 50, f))
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 150, f))
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 250, f))
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 350, f))
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 450, f))
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 550, f))
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 750, f))
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 1200, f))
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 3000, f))

    println("\nFirst execution...")
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 50, f))
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 150, f))
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 250, f))
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 350, f))
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 450, f))
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 550, f))
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 750, f))
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 1200, f))
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 3000, f))


    println("\nSecond execution...")
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 50, f))
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 150, f))
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 250, f))
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 350, f))
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 450, f))
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 550, f))
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 750, f))
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 1200, f))
    BenchmarkExecutor(name, new SetIntersectBenchmark(1000, 3000, f))
  }

  def intersectAndJoinWithGrowing(name:String, f: (Set[Long], Set[Long], Set[Long]) => Set[Long]) = {

    println("Warming up...")
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 50, f))
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 150, f))
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 250, f))
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 350, f))
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 450, f))
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 550, f))
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 750, f))
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 1200, f))
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 3000, f))

    println("\nFirst execution...")
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 50, f))
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 150, f))
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 250, f))
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 350, f))
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 450, f))
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 550, f))
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 750, f))
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 1200, f))
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 3000, f))


    println("\nSecond execution...")
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 50, f))
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 150, f))
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 250, f))
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 350, f))
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 450, f))
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 550, f))
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 750, f))
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 1200, f))
    BenchmarkExecutor(name, new SetIntersectWithGrowingSetBenchmark(1000, 3000, f))
  }

}
