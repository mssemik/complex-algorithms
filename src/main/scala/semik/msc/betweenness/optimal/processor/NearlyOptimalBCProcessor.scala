package semik.msc.betweenness.optimal.processor

import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import semik.msc.betweenness.optimal.predicate.NOInitBFSPredicate
import semik.msc.betweenness.optimal.struct.{NOBFSVertex, NOVertex}
import semik.msc.betweenness.optimal.struct.messages.{BFSBCExtendMessage, DFSPointer, NOMessage}
import semik.msc.bfs.BFSShortestPath
import semik.msc.utils.GraphSimplifier

import scala.Array._
import scala.reflect.ClassTag

/**
  * Created by mth on 5/7/17.
  */
class NearlyOptimalBCProcessor[VD, ED: ClassTag](graph: Graph[VD, ED]) extends Serializable {

  val initVertexId = graph.ops.pickRandomVertex()

  lazy val initGraph = initBFS

  private def initBFS = {
    val tempGraph = GraphSimplifier.simplifyGraph(graph)((a, _) => a).mapVertices((id, _) => NOVertex(id))
    val preparedGraph = Graph[NOVertex, ED](
      vertices = tempGraph.vertices,
      edges = tempGraph.edges,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK,
      edgeStorageLevel = StorageLevel.MEMORY_AND_DISK
    )
    val initBFSProcessor = new BFSShortestPath[NOVertex, ED, List[NOMessage[VertexId]]](new NOInitBFSPredicate, new NOInitBFSProcessor[ED]())
    val kk = initBFSProcessor.computeSingleSelectedSourceBFS(preparedGraph, initVertexId)

//    kk.vertices.foreach({ case (id, v) => println(s"For vertex $id -> pred: ${v.pred.get}, #succ: ${v.succ.getOrElse(empty).length}" ) })
    kk
  }

  def prepareVertices(startVertex: VertexId)(vertex: NOVertex) = vertex.vertexId match {
    case vId if startVertex == vId =>
      val nextVert = vertex.lowestSucc
      val pointer = Some(DFSPointer(startVertex, nextVert, toSent = true))
      val succ = updateSuccSet(vertex, pointer)
      vertex.update(succ = succ, dfsPointer = pointer, bfsMap = Map(startVertex -> NOBFSVertex(0, .0, 1)))
    case _ => vertex
  }

  def applyMessages(round: Int)(vertexId: VertexId, vertex: NOVertex, messages: Option[List[NOMessage[VertexId]]]) = {
    val msg = messages.getOrElse(List.empty)
    val pointer = msg.filter(_.isDFSPointer).map(_.asInstanceOf[DFSPointer])
    val bfsMsg = msg.filter(_.isExpand).map(_.asInstanceOf[BFSBCExtendMessage])

    val newPointer = updateDFSPointer(vertex, pointer.headOption)
    val newSucc = updateSuccSet(vertex, newPointer)
    val newBfsMap = updateBfsMap(vertex.bfsMap, bfsMsg)

    newPointer match {
      case Some(ptr) if !ptr.toSent =>
        val newBfs = (vertexId, NOBFSVertex(round + 1, .0, 1))
        val bfsMap = newBfsMap + newBfs
        vertex.update(succ = newSucc, dfsPointer = newPointer, bfsMap = bfsMap)
      case _ =>
        vertex.update(succ = newSucc, dfsPointer = newPointer, bfsMap = newBfsMap)
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

  def updateBfsMap(map: Map[VertexId, NOBFSVertex], messages: List[BFSBCExtendMessage]) = {
    val msgVertex = messages.groupBy(_.source)
      .map({ case (key, arr) => (key, NOBFSVertex(arr.head.startRound, arr.head.distance, arr.aggregate(0)((acc, m) => acc + m.sigma, _ + _))) })
    map ++ msgVertex
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

      srcAttr.bfsMap.foreach({ case (root, vertex) =>
        if (!dstAttr.bfsMap.contains(root) && round >= vertex.startRound) send(List(BFSBCExtendMessage.create(root, vertex)))})

    }

    val triplet = ctx.toEdgeTriplet
    val pointerSender = sendPointer(triplet) _
    pointerSender(ctx.srcId, ctx.sendToSrc)
    pointerSender(ctx.dstId, ctx.sendToDst)

    val extSender = sendBFSExtendMessage(triplet) _
    extSender(ctx.srcId, ctx.sendToSrc)
    extSender(ctx.dstId, ctx.sendToDst)
  }
}
