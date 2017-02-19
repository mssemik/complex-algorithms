package semik.msc.benchmark

import scala.collection.mutable

/**
  * Created by mth on 12/21/16.
  */
object SetBenchmark {

  def setMutable(nl: Int, ml: Int)(f: (mutable.Set[Int], Int) => mutable.Set[Int]) = {
    var times = List[Long]()
    for (n <- (0 to nl)) {
      var set = mutable.Set[Int]()
      val startTime = System.nanoTime
      for (i <- (0 to ml)) {
        set = f(set, i)
      }
      val stopTime = System.nanoTime
      times = (stopTime - startTime) :: times
    }
    println("Max: " + times.max + " | Min: " + times.min + " | Mean: " + (times.reduce((x, y) => x + y)) / times.size)
  }

  def setImmutable(nl: Int, ml: Int)(f: (Set[Int], Int) => Set[Int]) = {
    var times = List[Long]()
    for (n <- (0 to nl)) {
      var set = Set[Int]()
      val startTime = System.nanoTime
      for (i <- (0 to ml)) {
        set = f(set, i)
      }
      val stopTime = System.nanoTime
      times = (stopTime - startTime) :: times
    }
    println("Max: " + times.max + " | Min: " + times.min + " | Mean: " + (times.reduce((x, y) => x + y)) / times.size)
  }

  def sumOdIntersect = {
    val s1 = (0 to 2000).toSet
    val s2 = (1200 to 3200).toSet
    val s3 = (2700 to 4700).toSet

    var kk = Set[Int]()
    var sum = 0

    val startTime1 = System.nanoTime

    for (i <- (0 to 10000)) {
      kk = s2.intersect(s1 ++ s3)
      sum += kk.size
    }

    val stopTime1 = System.nanoTime

    println("1: " + (stopTime1 - startTime1))

    val startTime2 = System.nanoTime

    for (i <- (0 to 10000)) {
      kk = (s1 ++ s3).intersect(s2)
      sum += kk.size
    }

    val stopTime2 = System.nanoTime

    println("2: " + (stopTime2 - startTime2))

    val startTime3 = System.nanoTime

    for (i <- (0 to 10000)) {
      kk = s1.intersect(s2) ++ s1.intersect(s3)
      sum += kk.size
    }

    val stopTime3 = System.nanoTime

    println("3: " + (stopTime3 - startTime3))
  }

  def launchIntersectSets() = {
    val t = (0 to 100).toSet

    val interMu = (s: mutable.Set[Int], i: Int) => s.intersect(t)
    val interIm = (s: Set[Int], i: Int) => s.intersect(t)

    println("Warming up....")

    setMutable(10, 1000)(interMu)
    setImmutable(10, 1000)(interIm)

    println("First executing...")

    setMutable(10000, 100000)(interMu)
    setImmutable(10000, 100000)(interIm)

    println("Second executing...")

    setMutable(10000, 100000)(interMu)
    setImmutable(10000, 100000)(interIm)

    println("Third executing...")

    setMutable(10000, 100000)(interMu)
    setImmutable(10000, 100000)(interIm)
  }

  def launchGrowingSets() = {

    val growIm = (s: Set[Int], i: Int) => s + i
    val growMu = (s: mutable.Set[Int], i: Int) => {
      s += i
      s
    }

    println("Warming up....")

    setMutable(10, 1000)(growMu)
    setImmutable(10, 1000)(growIm)

    println("First executing...")

    setMutable(10000, 100000)(growMu)
    setImmutable(10000, 100000)(growIm)

    println("Second executing...")

    setMutable(10000, 100000)(growMu)
    setImmutable(10000, 100000)(growIm)

    println("Third executing...")

    setMutable(10000, 100000)(growMu)
    setImmutable(10000, 100000)(growIm)

  }

  def main(args: Array[String]): Unit = {
//    launchIntersectSets
//    launchGrowingSets

    for (k <- (0 to 10)) {
      sumOdIntersect
    }
  }
}
