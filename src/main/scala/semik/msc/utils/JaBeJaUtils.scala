package semik.msc.utils

import org.apache.spark.graphx.Edge

import scala.reflect.ClassTag
import scala.util.Random

/**
  * Created by mth on 1/26/17.
  */
object JaBeJaUtils {

  def buildArray[T: ClassTag](len: Int)(index: Int, value: T): Array[T] = {
    val array = Array.ofDim[T](len)
    array(index) = value
    array
  }

  def sumArrays[T: ClassTag](arr1: Array[T], arr2: Array[T])(sum: (T, T) => T) = {
    require(arr1.size == arr2.size)

    val result = Array.ofDim[T](arr1.size)
    for (i <- (0 to arr2.length - 1))
      result(i) = sum(arr1(i), arr2(1))

    result
  }

  def getRandomElem[T](list: Seq[T], rand: Random = Random) = if (list.isEmpty) None else Some(list(rand.nextInt(list.size)))

  def getRandomElems[T](list: Seq[T], len: Int = Int.MaxValue, rand: Random = Random) =
    if (list.size > len)
      rand.shuffle(list).take(len)
    else list

  def isInternal(edges: Seq[Edge[Int]]) = {
    val headColor = if (edges.isEmpty) -1 else edges.head.attr
    edges.forall(_.attr == headColor)
  }

  def getDominantColor(edges: Seq[Edge[Int]]) =
    edges.groupBy(_.attr).maxBy(_._2.size)._1

  def edgeEquals(e1: Edge[_], e2: Edge[_]) =
    if (e1.srcId == e2.srcId)
      e1.dstId == e2.dstId
    else if (e1.srcId == e2.dstId)
      e1.dstId == e2.srcId
    else false

}
