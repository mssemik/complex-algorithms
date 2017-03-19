package semik.msc.random.ctrw

import java.io.Serializable

import org.apache.spark.graphx._
import semik.msc.neighbourhood.VertexNeighbourhood
import semik.msc.tools.GraphSimplifier

import scala.util.Random

/**
  * Created by mth on 3/1/17.
  */
class ContinuousTimeRandomWalk[VD, ED](graph: Graph[VD, ED]) extends Serializable {

  val graphSimplifier = new GraphSimplifier[VD, ED](graph)

  lazy val simpleGraph = graphSimplifier.simpleNaiveGraph

  private def prepareVertexData(initTemp: Double, numRandomVertex: Int) = {
    val nearestNbh = VertexNeighbourhood(simpleGraph).nearestNeighbourhood
    val graphWithNbh = simpleGraph.outerJoinVertices(nearestNbh)((id, _, nbh) => nbh.getOrElse(Array.empty))
    graphWithNbh.mapVertices((id, v) => {
      val ctrwMsg = for (i <- 0 until numRandomVertex) yield {
        val nextVertex = v(Random.nextInt(v.size))
        CTRWMessage(id, initTemp, List(nextVertex, id))
      }
      CTRWVertex(v, ctrwMsg.toList, List.empty)
    })
  }

  private def mergeMessages(id: VertexId, v: CTRWVertex, msg: List[CTRWMessage]) = {
    val rand = Random
    val newMsg = msg.map(ms =>
      if (ms.temperature > 0) {
        val temp = ms.temperature + (Math.log(rand.nextDouble()) / v.degree)
        val stack = if (temp <= 0) ms.path.tail else v.nbh(rand.nextInt(v.degree)) :: ms.path
        val newId = if (temp <= 0) id else ms.id
        CTRWMessage(newId, temp, stack)
      } else
        CTRWMessage(ms.id, ms.temperature, ms.path.tail)
    )

    val completedIds = newMsg.filter(_.path.isEmpty).map(_.id)
    val active = newMsg.filter(_.path.nonEmpty)
    CTRWVertex(v.nbh, active, v.selectedVertices ++ completedIds)
  }

  private def sendMsg(triplet: EdgeContext[CTRWVertex, _, List[CTRWMessage]]) = {
    val filter: (VertexId) => List[CTRWMessage] = filterMsgVertexFrom(triplet.toEdgeTriplet)
    triplet.sendToDst(filter(triplet.dstId))
    triplet.sendToSrc(filter(triplet.srcId))
  }

  private def filterMsgVertexFrom(triplet: EdgeTriplet[CTRWVertex, _])(other: VertexId) = {
    val otherAttr = triplet.otherVertexAttr(other)
    otherAttr.msg.filter(_.path.head == other)
  }

  def chooseRandomVertices(initTemp: Double, numberOfVertex: Int = 1) = {
    var g = prepareVertexData(initTemp, numberOfVertex).cache()
    var messages = g.aggregateMessages[List[CTRWMessage]](sendMsg(_), _ ++ _)
    var activeMessages = messages.count()
    var prevG = g
    while (activeMessages > 0) {
      prevG = g
      g = g.ops.joinVertices(messages)(mergeMessages(_, _, _)).cache()

      val oldMessages = messages
      messages = g.aggregateMessages[List[CTRWMessage]](sendMsg(_), _ ++ _).cache()
      activeMessages = messages.map(_._2.size).reduce(_ + _)

      val kkk = messages.flatMap(v => v._2.map(m => m.temperature))

      val min = if (kkk.isEmpty()) 0 else kkk.min()
      val max = if (kkk.isEmpty()) 0 else kkk.max()
      val mean = if (kkk.isEmpty()) 0 else kkk.mean()

      println("Max: " + max + ", min: " + min + ", mean: " + mean + ", #msg: " + activeMessages)

      oldMessages.unpersist(false)
      prevG.unpersist(false)
    }
    messages.unpersist(false)
    g.mapVertices((id, v) => v.selectedVertices)
  }

  class CTRWMessage(val id: VertexId, val temperature: Double, val path: List[VertexId]) extends Serializable

  object CTRWMessage extends Serializable {
    def apply(
               id: VertexId,
               temperature: Double,
               path: List[VertexId] = List()
             ): CTRWMessage = new CTRWMessage(id, temperature, path)
  }

  class CTRWVertex(val nbh: Array[VertexId], val msg: List[CTRWMessage], val selectedVertices: List[VertexId]) extends Serializable {
    val degree = nbh.size
  }

  object CTRWVertex extends Serializable {
    def apply(
               nbh: Array[VertexId],
               msg: List[CTRWMessage],
               selectedVertices: List[VertexId]
             ): CTRWVertex = new CTRWVertex(nbh, msg, selectedVertices)
  }

}

