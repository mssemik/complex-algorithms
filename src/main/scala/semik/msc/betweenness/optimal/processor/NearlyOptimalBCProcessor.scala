package semik.msc.betweenness.optimal.processor

import org.apache.spark.graphx._
import semik.msc.betweenness.optimal.predicate.NOInitBFSPredicate
import semik.msc.betweenness.optimal.struct.messages._
import semik.msc.betweenness.optimal.struct.{NOBFSVertex, NOVertex}
import semik.msc.bfs.BFSShortestPath

import scala.Array._
import scala.reflect.ClassTag

/**
  * Created by mth on 5/7/17.
  */
class NearlyOptimalBCProcessor[VD, ED: ClassTag](graph: Graph[VD, ED]) extends Serializable {

  val initVertexId = graph.ops.pickRandomVertex()

  lazy val initGraph = initBFS

  val testId = 5
  val testId2 = 56

  private def initBFS = {
    val preparedGraph = graph.mapVertices((id, _) => NOVertex(id))
    val initBFSProcessor = new BFSShortestPath[NOVertex, ED, List[NOMessage[VertexId]]](new NOInitBFSPredicate, new NOInitBFSProcessor[ED]())
    initBFSProcessor.computeSingleSelectedSourceBFS(preparedGraph, initVertexId)
  }

  def prepareVertices(startVertex: VertexId)(vertex: NOVertex) = vertex.vertexId match {
    case vId if startVertex == vId =>
      val nextVert = vertex.lowestSucc
      val pointer = Some(DFSPointer(startVertex, nextVert, toSent = true))
      val succ = updateSuccSet(vertex, pointer)
      vertex.update(succ = succ, dfsPointer = pointer, bfsMap = Map(startVertex -> NOBFSVertex(0, .0, 1, state = NOBFSVertex.toConfirm)))
    case _ => vertex
  }

  def applyMessages(round: Int)(vertexId: VertexId, vertex: NOVertex, messages: Option[List[NOMessage[VertexId]]]) = {
    val msg = messages.getOrElse(List.empty)
    val pointer = msg.filter(_.isDFSPointer).map(_.asInstanceOf[DFSPointer])
    val bfsMsg = msg.filter(m => m.isExpand || m.isConfirm || m.isAggregation)

    val newPointer = updateDFSPointer(vertex, pointer.headOption)
    val newSucc = updateSuccSet(vertex, newPointer)
    val newBfsMap = updateBfsMap(vertexId, vertex.bfsMap, bfsMsg)

    val toRemove = newBfsMap.filter({ case (key, value) => value.isCompleted && key != vertexId })

    val bcIncrees = toRemove.values.map(n => n.psi * n.sigma.toDouble).sum

    newPointer match {
      case Some(ptr) if !ptr.toSent =>
        val newBfs = (vertexId, NOBFSVertex(ptr.round + 1, .0, 1, .0, 0, 0, NOBFSVertex.idle))
        val bfsMap = newBfsMap + newBfs
        vertex.update(succ = newSucc, dfsPointer = newPointer, bfsMap = bfsMap, bcInc = bcIncrees)
      case _ =>
        vertex.update(succ = newSucc, dfsPointer = newPointer, bfsMap = newBfsMap, bcInc = bcIncrees)
    }
  }

  def updateDFSPointer(vertex: NOVertex, pointerMsg: Option[DFSPointer]): Option[DFSPointer] =
    vertex.dfsPointer match {
      case Some(pointer) if pointer.toRemove => None
      case Some(pointer) if !vertex.leaf => Some(pointer.asToSent())
      case Some(pointer) if vertex.leaf => Some(pointer.asReturning)
      case None => pointerMsg match {
        case Some(pointer) if pointer.returning && vertex.leaf => pointerMsg
        case Some(pointer) if pointer.returning && !vertex.leaf => Some(pointer.asToSent(vertex.lowestSucc))
        case Some(pointer) => Some(pointer.asWaiting(vertex.lowestSucc))
        case _ => None
      }
    }

  def updateSuccSet(vertex: NOVertex, pointer: Option[DFSPointer]): Option[Array[VertexId]] = pointer match {
    case Some(p) if p.next.nonEmpty && vertex.succ.nonEmpty =>
      Some(vertex.succ.getOrElse(empty[VertexId]).filterNot(p.next.contains(_)))
    case _ => vertex.succ
  }

  def updateBfsMap(vertexId: VertexId, map: Map[VertexId, NOBFSVertex], messages: List[NOMessage[VertexId]]) = {
//    def buildMsgTriple(msgList: List[NOMessage[VertexId]]) = {
//      val map = msgList.groupBy(m => (m.isExpand, m.isConfirm, m.isAggregation))
//      val expandMessages = map.getOrElse((true, false, false), List()).map(_.asInstanceOf[BFSBCExtendMessage])
//      val confirmMessages = map.getOrElse((false, true, false), List()).map(_.asInstanceOf[BFSBCConfirmMessage])
//      val aggregationMessages = map.getOrElse((false, false, true), List()).map(_.asInstanceOf[BCAggregationMessage])
//      (expandMessages, confirmMessages, aggregationMessages)
//    }

//    val filteredMap = map.filterNot({ case (key, value) => value.isCompleted })
//    if (vertexId == testId) println(s"Z $testId usunieto: ${map.filter(_._2.isCompleted).keySet.aggregate("")((acc, v) => acc + ", " + v, _ + _)}")
    val msgMap = messages.groupBy(_.source)

//    val msgVertex = msgMap
//      .map({ case (key, l) => (key, buildMsgTriple(l)) })
//      .map({ case (key, (exp, cnf, aggr)) =>
//        filteredMap.get(key) match {
//          case Some(vert) if vert.state == NOBFSVertex.idle => (key, vert.setToConfirm)
//          case Some(vert) if vert.state == NOBFSVertex.toConfirm => (key, vert.waitForConfirm)
//          case Some(vert) if vert.state == NOBFSVertex.waitForConfirm =>
////            if (key == testId) println(s"$vertexId odebral conf od ${cnf.aggregate("")((acc, j) => acc + ", " + j.content, _ + _)}, zrodlo: $key")
//            (key, vert.applyConfirmations(cnf))
//          case Some(vert) if vert.state == NOBFSVertex.confirmed && aggr.nonEmpty =>
////            if (key == testId) println(s"$vertexId odebral aggr od ${aggr.aggregate("")((acc, j) => acc + ", " + j.content, _ + _)}, zrodlo: $key")
//            val properlyAggreagation = aggr.filterNot(_.source == vertexId)
//            val bcValues = aggr.map(_.psi)
//            (key, vert.updateBC(bcValues))
//          case Some(vert) => (key, vert)
//          case None =>
////            if (key == testId) println(s"$vertexId odebralo ext od ${exp.aggregate("")((acc, v) => acc + ", " + v.content, _ + _)} o zrodle $key")
//            val sigma = exp.map(_.sigma).sum
//            (key, NOBFSVertex(exp.head.startRound, exp.head.distance, sigma, .0, 0, 0, NOBFSVertex.toConfirm))
//        }
//      })

    val filteredFlowsMap = map.filter({ case (root, flow) => !flow.isCompleted})
    val expandMessages = messages.filter(_.isExpand).groupBy(_.source)

    val removedByAccident = map.filter({ case (root, flow) => flow.isCompleted && expandMessages.contains(root)})
    if (removedByAccident.nonEmpty) println(s"${removedByAccident.size} removed by accident")

    val msgVertex2 = filteredFlowsMap.map({ case (root, flow) =>
      val messages = msgMap.getOrElse(root, List.empty)
      flow.state match {
        case NOBFSVertex.idle =>
          (root, flow.setToConfirm)
        case NOBFSVertex.toConfirm if messages.nonEmpty =>
          throw new Error("Unsuspected messages is state toConfirm")
        case NOBFSVertex.toConfirm =>
          (root, flow.waitForConfirm)
        case NOBFSVertex.waitForConfirm if messages.exists(m => m.isAggregation || m.isExpand) =>
          throw new Error("Unsuspected messages is state waitForConfirm")
        case NOBFSVertex.waitForConfirm =>
          val confirmations = messages.map(_.asInstanceOf[BFSBCConfirmMessage])
          (root, flow.applyConfirmations(confirmations))
        case NOBFSVertex.confirmed if messages.exists(m => m.isExpand || m.isConfirm) =>
          throw new Error("Unsuspected messages is state confirmed")
        case NOBFSVertex.confirmed =>
          messages.foreach({ case m: BCAggregationMessage =>
              println(s"R ${m.msgSource} => $vertexId, $root")
          })
          val aggregations = messages.map(_.asInstanceOf[BCAggregationMessage].psi)
          (root, flow.updateBC(aggregations))
        case _ => throw new Error("Unsuspected case")
      }
    })

    val createdFlows = expandMessages.map({ case (root, expand: List[BFSBCExtendMessage]) =>
      if (msgVertex2.contains(root)) throw new Error("Attempt to create duplicate of vertex")
      val sigma = expand.map(_.sigma).sum
      val startRound = expand.head.startRound
      val distance = expand.head.distance
      val vertex = NOBFSVertex(startRound, distance, sigma, state = NOBFSVertex.toConfirm)
      (root, vertex)
    })

//    val pp = (filteredMap -- msgMap.keySet).map({ case (key, value) =>
//      value.state match {
//        case NOBFSVertex.idle => (key, value.setToConfirm)
//        case NOBFSVertex.toConfirm => (key, value.waitForConfirm)
//        case NOBFSVertex.waitForConfirm => (key, value.applyConfirmations(List.empty))
//        case _ => (key, value)
//      }
//    })

    msgVertex2 ++ createdFlows
  }

  def sendMessages(round: Int)(ctx: EdgeContext[NOVertex, ED, List[NOMessage[VertexId]]]): Unit = {
    def sendPointer(triplet: EdgeTriplet[NOVertex, ED])(dst: VertexId, send: (List[NOMessage[VertexId]]) => Unit) = {
      val srcAttr = triplet.otherVertexAttr(dst)
      srcAttr.dfsPointer match {
        case Some(pointer) if pointer.returning && pointer.toSent && srcAttr.pred.contains(dst) => send(List(pointer))
        case Some(pointer) if pointer.toSent && pointer.next.contains(dst) /*&& !dstAttr.bfsRoot*/ => send(List(pointer))
        case _ =>
      }
    }

    def sendBFSExtendMessage(triplet: EdgeTriplet[NOVertex, ED])(dst: VertexId, send: (List[NOMessage[VertexId]]) => Unit) = {
      val srcAttr = triplet.otherVertexAttr(dst)
      val dstAttr = triplet.vertexAttr(dst)
      val srcPtrRound = srcAttr.dfsPointer.map(_.round).getOrElse(Long.MaxValue)

      srcAttr.bfsMap.foreach({ case (root, vertex) =>
        if (!dstAttr.bfsMap.contains(root) && /*srcPtrRound >= vertex.startRound &&*/ vertex.state == NOBFSVertex.toConfirm)
          send(List(BFSBCExtendMessage.create(root, vertex)))
      })

    }

    def sendConfirmation(triplet: EdgeTriplet[NOVertex, ED])(dst: VertexId, send: (List[NOMessage[VertexId]]) => Unit) = {
      val srcAttr = triplet.otherVertexAttr(dst)
      val dstAttr = triplet.vertexAttr(dst)

      srcAttr.bfsMap.filter({ case (key, v) => v.state == NOBFSVertex.toConfirm })
        .foreach({ case (key, v) =>
          dstAttr.bfsMap.get(key) match {
            case Some(parent) if isParentWaitingForConfirm(v, parent) =>
              send(List(BFSBCConfirmMessage(key, srcAttr.vertexId)))
            case _ =>
          }
        })


    }

    def isParentWaitingForConfirm(vert: NOBFSVertex, parent: NOBFSVertex) =
      isParent(vert, parent) && parent.state == NOBFSVertex.waitForConfirm

    def isParent(vert: NOBFSVertex, parent: NOBFSVertex) = vert.distance == parent.distance + 1

    def sendAggregate(triplet: EdgeTriplet[NOVertex, ED])(dst: VertexId, send: (List[NOMessage[VertexId]]) => Unit) = {
      val srcAttr = triplet.otherVertexAttr(dst)
      val dstAttr = triplet.vertexAttr(dst)

      srcAttr.bfsMap.filter({ case (key, v) => dstAttr.bfsMap.get(key).exists(p => isParent(v, p)) && v.isCompleted})
        .foreach({ case (key, v) =>
          println(s"S ${srcAttr.vertexId} => $dst, $key")
          send(List(BCAggregationMessage(key, srcAttr.vertexId, 1.0 / v.sigma.toDouble + v.psi)))
        })
    }

    val triplet = ctx.toEdgeTriplet
    val pointerSender = sendPointer(triplet) _
    pointerSender(ctx.srcId, ctx.sendToSrc)
    pointerSender(ctx.dstId, ctx.sendToDst)

    val extSender = sendBFSExtendMessage(triplet) _
    extSender(ctx.srcId, ctx.sendToSrc)
    extSender(ctx.dstId, ctx.sendToDst)

    val confirmSender = sendConfirmation(triplet) _
    confirmSender(ctx.srcId, ctx.sendToSrc)
    confirmSender(ctx.dstId, ctx.sendToDst)

    val aggregationSender = sendAggregate(triplet) _
    aggregationSender(ctx.srcId, ctx.sendToSrc)
    aggregationSender(ctx.dstId, ctx.sendToDst)
  }
}
