package semik.msc.random

import java.io.Serializable

import org.apache.spark.graphx._
import sun.util.resources.cldr.pa.CurrencyNames_pa

import scala.math.Ordering
import scala.util.Random

/**
  * Created by mth on 3/1/17.
  */
class ContinuousTimeRandomWalk[VD, ED](graph: Graph[VD, ED]) extends Serializable {

  val collectedNeighbourIds =
    graph.outerJoinVertices(graph.ops.collectNeighborIds(EdgeDirection.Either))((id, _, nb) => nb.map(_.toList).getOrElse(List.empty))

  def prepareVertexData(initTemp: Double, numOfVertex: Int) =
    collectedNeighbourIds.ops.convertToCanonicalEdges((_, e) => e).ops.removeSelfEdges().mapVertices((id, nbs) =>
      (nbs, List(CTRWMessage(id, initTemp, List(id))), List[CTRWMessage]()))

  def joinVeticesWithMessages(id: VertexId, v: (List[VertexId], List[CTRWMessage], List[CTRWMessage]), msg: List[CTRWMessage]) = {
    val rand = Random
    val newMsg = msg.map( n=>

        if (n.temperature > 0) {
          val temp = n.temperature + (Math.log(rand.nextDouble()) / v._1.size)
          val stack = v._1(rand.nextInt(v._1.size)) :: n.path
          CTRWMessage(n.id, temp, stack)
        } else
          CTRWMessage(n.id, n.temperature, n.path.tail)
    )

    newMsg.foreach(p => println(p.temperature + " -> " + p.path.size))

    val finished = newMsg.filter(_.path.isEmpty).map(m => CTRWMessage(id, m.temperature, m.path))
    val active = newMsg.filter(_.path.nonEmpty)
    (v._1, active, finished ++ v._3)
  }

  def sendMsg(triplet: EdgeContext[(List[VertexId], List[CTRWMessage], List[CTRWMessage]), _, List[CTRWMessage]]) = {
    val msgToSrc = triplet.dstAttr._2.filter(_.path.headOption match {
      case Some(id) => id == triplet.srcId
      case _ => false
    })

    val msgToDst = triplet.dstAttr._2.filter(_.path.headOption match {
      case Some(id) => id == triplet.dstId
      case _ => false
    })

    val revertedMsg = msgToSrc.filter(_.temperature <= 0) ++ msgToDst.filter(_.temperature <= 0)
    val forwardedMessages = msgToSrc.filter(_.temperature > 0) ++ msgToDst.filter(_.temperature > 0)
    (revertedMsg.map(m => (m.path.head, List(m))) ++ forwardedMessages.map(m => (m.path.head, List(m)))).foreach(m => {
      if (m._1 == triplet.srcId)
        triplet.sendToSrc(m._2)
      else
        triplet.sendToDst(m._2)
    })
  }

  def designateRandomVertices(initTemp: Double) = {
    var g = prepareVertexData(initTemp, 1).cache()
    var messages = g.aggregateMessages[List[CTRWMessage]](f => sendMsg(f), _ ++ _)
    var activeMessages = messages.count()
    var prevG = g
    var i = 0
    while (activeMessages > 0) {
      prevG = g
      g = g.ops.joinVertices(messages)(joinVeticesWithMessages(_, _, _)).cache()
      //
      val oldMessages = messages
      messages = g.aggregateMessages[List[CTRWMessage]](f => sendMsg(f), _ ++ _).cache()
      activeMessages = messages.count()
      oldMessages.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)
    }
    messages.unpersist(blocking = false)
    g
  }

}

class CTRWMessage(val id: VertexId, val temperature: Double, val path: List[VertexId]) extends Serializable

class CTRWInitMessage extends CTRWMessage(0, 0.0, List.empty)

case object CTRWInitMessage extends Serializable {
  def apply: CTRWInitMessage = new CTRWInitMessage()

  def unapply(arg: CTRWInitMessage): Option[CTRWInitMessage] = Option(arg)
}

object CTRWMessage extends Serializable {
  def apply(
             id: VertexId,
             temperature: Double,
             path: List[VertexId] = List()
           ): CTRWMessage = new CTRWMessage(id, temperature, path)

  def unapply(arg: CTRWMessage): Option[CTRWMessage] = Option(arg)
}
