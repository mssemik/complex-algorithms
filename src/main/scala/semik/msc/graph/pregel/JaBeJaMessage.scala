package semik.msc.graph.pregel

import semik.msc.utils.JaBeJaUtils._

/**
  * Created by mth on 2/2/17.
  */
object JaBeJaMessage {
  def initMessage = new JaBeJaInit
  def dummyMessage = new JaBeJaDummy
  def edgeColorAggregation(numOfPart: Int, currColor: Int) = new JaBeJaEdgeColorAggregation(buildArray(numOfPart)(currColor, 1))

}

trait JaBeJaMessage extends Serializable {
  def toContainer = new JaBeJaContainer(List(this))
}

class JaBeJaDummy extends JaBeJaMessage

class JaBeJaInit extends JaBeJaMessage

class JaBeJaContainer(val list: List[JaBeJaMessage]) extends JaBeJaMessage {
  def add(other: List[JaBeJaMessage]): JaBeJaContainer = new JaBeJaContainer(list ++ other)

  def add(other: JaBeJaMessage): JaBeJaContainer = other match {
    case c: JaBeJaContainer => add(c.list)
    case _                  => add(List(other))
  }

  override def toContainer = this
}

class JaBeJaEdgeColorAggregation(val colors:Array[Int]) extends JaBeJaMessage {
  def join(other: Array[Int]) = new JaBeJaEdgeColorAggregation(sumArrays(colors, other)(_ + _))
}

