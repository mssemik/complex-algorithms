package semik.msc.random

import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import semik.msc.random.struct.{RandomWeightWalkNeighbour, RandomWeightWalkVertex}
import semik.msc.utils.RandomWeightWalkUtils._

import scala.util.Random

/**
  * Created by mth on 2/6/17.
  */
class RandomWeightWalk(quantum: Double = 0.02) extends Serializable {

  def initGraph[VD, ED](graph: Graph[VD, ED]) = initializeVertices(prepareGraph(graph))

  def selNbh[ED](graph: Graph[RandomWeightWalkVertex, ED]) = selectNeighbours(graph, quantum)

  def aggrIncMsg[ED](graph: Graph[RandomWeightWalkVertex, ED]) = graph.aggregateMessages[List[VertexId]](
    eCtx => {

      if (eCtx.srcAttr.vertexWeight >= quantum && eCtx.srcAttr.neighbours.exists(n => n.vertexState != RWWVertexState.completed)) {
        eCtx.srcAttr.selected match {
          case Some(sel) => if (sel == eCtx.dstId) eCtx.sendToDst(List(eCtx.srcId))
          case _ =>
        }
      }

      if (eCtx.dstAttr.vertexWeight >= quantum && eCtx.dstAttr.neighbours.exists(n => n.vertexState != RWWVertexState.completed)) {
        eCtx.dstAttr.selected match {
          case Some(sel) => if (sel == eCtx.srcId) eCtx.sendToSrc(List(eCtx.dstId))
          case _ =>
        }
      }
    }, _ ++ _
  )

  def updIncMsg[ED](graph: Graph[RandomWeightWalkVertex, ED], mmm:VertexRDD[List[VertexId]]) =
    graph.ops.joinVertices(mmm)((id, v, msgList) => {
      val isNbSel = v.selected.nonEmpty
      val isComp = v.vertexWeight < quantum || v.neighbours.forall(_.vertexState == RWWVertexState.completed)
      val zeroVal = if (isNbSel && !isComp) (v.vertexWeight - quantum, Set[VertexId]()) else (v.vertexWeight, Set[VertexId]())
      val msgSet = msgList.toSet

      val (newWeight, accMsg) = msgSet.foldLeft(zeroVal)(
        (acc, msg) => if (acc._1 >= quantum) (acc._1 - quantum, acc._2 + msg) else acc
      )
      val rvMsg = msgSet.diff(accMsg)

      accMsg.foreach(nbId => {
        val nb = v.neighbour(nbId)
        val nnb = RandomWeightWalkNeighbour(nb.vertexId, nb.weight + quantum, nb.vertexState)
        v.updateNeighbour(nnb)
      })

      val resp = if (msgList.isEmpty) None else Some(accMsg.map(d => (d, true)) ++ rvMsg.map(d => (d, false)))
      val rw = if (isNbSel && !isComp) newWeight + quantum else newWeight

      RandomWeightWalkVertex(rw, v.neighbours, v.selected, resp)
    })

  def aggrResp[ED](graph: Graph[RandomWeightWalkVertex, ED]) = graph.aggregateMessages[Boolean](
    eCtx => {
      eCtx.srcAttr.responses.getOrElse(Set.empty).find(n => n._1 == eCtx.dstId).foreach(m => eCtx.sendToDst(m._2))
      eCtx.dstAttr.responses.getOrElse(Set.empty).find(n => n._1 == eCtx.srcId).foreach(m => eCtx.sendToSrc(m._2))
    }, _ && _
  )

  def updResp[ED](graph: Graph[RandomWeightWalkVertex, ED], mmm:VertexRDD[Boolean]) = graph.outerJoinVertices(mmm)((id, v, res) => {
    val updated = v.selected match {
      case Some(nId) =>
        val n = v.neighbour(nId)
        val nbWeight = if (res.getOrElse(false)) n.weight + quantum else n.weight
        val newVW = if (res.getOrElse(false)) v.vertexWeight - quantum else v.vertexWeight
        val nbState = if (res.getOrElse(false)) RWWVertexState.ready else RWWVertexState.completed
        val nb = RandomWeightWalkNeighbour(n.vertexId, nbWeight, nbState)
        v.updateNeighbour(nb)
        RandomWeightWalkVertex(newVW, v.neighbours, None, None)
      case _ => RandomWeightWalkVertex(v.vertexWeight, v.neighbours, None, None)
    }

    if (updated.vertexWeight < quantum) updated
    else {
      val activeNbs = updated.neighbours.filter(nb => nb.vertexState != RWWVertexState.completed)
      if (activeNbs.isEmpty) updated
      else{
        val selected = activeNbs(Random.nextInt(activeNbs.length))
        RandomWeightWalkVertex(updated.vertexWeight, updated.neighbours, Some(selected.vertexId))
      }
    }
  })

  def prepareWalk[VD2, ED2](graph: Graph[VD2, ED2]) = {
    val initG = initGraph(graph)

    if (!initG.vertices.filter(v => v._2.vertexWeight < 0).isEmpty()) {
      throw new Error("Negative init weight")
    }

    var g = selNbh(initG).cache()

    var messages = aggrIncMsg(g).cache()
    var activeMessages = messages.count()
    var prevG: Graph[RandomWeightWalkVertex, _] = null
    var i = 1

    while (activeMessages > 0) {
      prevG = g
      g = updIncMsg(g, messages).cache()

      val m1 = aggrResp(g).cache()
      activeMessages = m1.count()

      messages.unpersist(false)
      prevG.unpersist(false)

      prevG = g
      g = updResp(g, m1).cache()

      messages = aggrIncMsg(g).cache()
      activeMessages = messages.count()

      println("Iteration " + i + " finished")

      val f = g.vertices.map(v => v._2.vertexWeight).cache()
      val nocn = g.vertices.filter(v => v._2.vertexWeight < quantum || v._2.neighbours.forall(n => n.vertexState == RWWVertexState.completed)).count()
      println("Min weight: " + f.min())
      println("Max weight: " + f.max())
      println("Mean weight: " + f.mean())
      println("Number of completed nodes: " + nocn)
      println("activeMessages: " + activeMessages)

      if (nocn > 80) {
        val c = g.vertices.filter(v => v._2.vertexWeight < quantum || v._2.neighbours.forall(n => n.vertexState == RWWVertexState.completed)).collect()
        c.foreach(v => {
          println("id: " + v._1 + ", w  = " + v._2.vertexWeight)
          println("number of active nb: " + v._2.neighbours.count(n => n.vertexState != RWWVertexState.completed))
          println("########################")
        })
      }

      i = i + 1

      prevG.unpersist(false)
      m1.unpersist(false)
    }

    messages.unpersist(false)
    g
  }

}
