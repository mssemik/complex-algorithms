package semik.msc.benchmark

/**
  * Created by mth on 12/21/16.
  */
object SetListBenchmark {

  def listIm(nl: Int, ml: Int)(f: (List[Int], Int) => List[Int]) = {
    var times = List[Long]()
    for (n <- (0 to nl)) {
      var l = List[Int]()
      val startTime = System.nanoTime
      for (i <- (0 to ml)) {
        l = i :: l
        f(l, i)
      }
      val stopTime = System.nanoTime
      times = (stopTime - startTime) :: times
    }
    println("Max: " + times.max + " | Min: " + times.min + " | Mean: " + (times.reduce((x, y) => x + y)) / times.size)
  }

  def setIm(nl: Int, ml: Int)(f: (Set[Int], Int) => Set[Int]) = {
    var times = List[Long]()
    for (n <- (0 to nl)) {
      var set = Set[Int]()
      val startTime = System.nanoTime
      for (i <- (0 to ml)) {
        set = set + i
        f(set, i)
      }
      val stopTime = System.nanoTime
      times = (stopTime - startTime) :: times
    }
    println("Max: " + times.max + " | Min: " + times.min + " | Mean: " + (times.reduce((x, y) => x + y)) / times.size)
  }


  def launchIntersect = {
    val tL = (0 to 100).toList
    val tS = (0 to 100).toSet

    val fL = (l: List[Int], i: Int) => l.intersect(tL)
    val fs = (s: Set[Int], i: Int) => s.intersect(tS)

    println("Warming up....")

    listIm(10, 1000)(fL)
    setIm(10, 1000)(fs)

    println("First executing...")

    listIm(100, 10000)(fL)
    setIm(100, 10000)(fs)

    println("Second executing...")

    listIm(100, 10000)(fL)
    setIm(100, 10000)(fs)

    println("Third executing...")

    listIm(100, 10000)(fL)
    setIm(100, 10000)(fs)
  }

  def main(args: Array[String]): Unit = {
    launchIntersect
  }
}
