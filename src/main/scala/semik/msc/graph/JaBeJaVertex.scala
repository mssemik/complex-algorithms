package semik.msc.graph

import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 1/16/17.
  */
class JaBeJaVertex(val color: Int, val neighbourColors: Array[Int], val bestNeighbour: Option[VertexId], val index: Double) extends Serializable {

  def this() = this(-1, Array(), None, 0.0)

  def this(color: Int) = this(color, Array(), None, 0.0)

  def changeColor(newColor: Int) = new JaBeJaVertex(newColor, neighbourColors, bestNeighbour, index)

  def setBestNeighbour(bestNeighbour: Option[VertexId], index: Double) = new JaBeJaVertex(color, neighbourColors, bestNeighbour, index)

  def setNeighbourColors(neighbourColors: Array[Int]) = new JaBeJaVertex(color, neighbourColors, bestNeighbour, index)

}
