//package semik.msc.partitioning
//
//import org.apache.spark.graphx._
//import semik.msc.graph.JaBeJaNeighbour
//import semik.msc.graph.pregel.JaBeJaMessage
//import semik.msc.partitioning.config.JaBeJaVCConfig
//import semik.msc.utils.JaBeJaUtils._
//
//import scala.util.Random
//
///**
//  * Created by mth on 1/28/17.
//  */
//class JaBeJaVC(config: JaBeJaVCConfig) extends Serializable {
//
//  def partition[VD, ED](graph: Graph[VD, ED]) = {
//    val jaBeJavc = graph.mapEdges(e => Random.nextInt(config.numOfPartition))
//
//  }
//
//  def partitionWithPregel(graph: Graph[JaBeJaMessage, Int]) = {
//    graph.pregel[JaBeJaMessage](JaBeJaMessage.initMessage)(
//      pregelApply(_, _, _),
//      pregelSend(_),
//      pregelMerge(_, _))
//  }
//
//  def pregelApply(vertexId: VertexId, vertexData: JaBeJaMessage, message: JaBeJaMessage): JaBeJaMessage = {
//    JaBeJaMessage.dummyMessage
//  }
//
//  def pregelSend(triplet: EdgeTriplet[JaBeJaMessage, Int]): Iterator[(VertexId, JaBeJaMessage)] = {
//
//    Iterator.empty
//  }
//
//  def pregelMerge(msg1: JaBeJaMessage, msg2: JaBeJaMessage) = msg1.toContainer.add(msg2)
//
//  def partitionByVerticesMap[VD](graph: Graph[VD, Int]) = {
//    val m1 = graph.aggregateMessages[Array[Int]](
//      edgeContext => {
//        val msg = buildArray(config.numOfPartition)(edgeContext.attr, 1)
//        edgeContext.sendToDst(msg)
//        edgeContext.sendToSrc(msg)
//      },
//      sumArrays(_, _)(_ + _),
//      TripletFields.EdgeOnly
//    )
//
//    val g1 = graph.outerJoinVertices(m1)((_, _, arr) => arr.getOrElse(Array.fill(config.numOfPartition)(0)))
//
//    val m2 = g1.aggregateMessages[Seq[JaBeJaNeighbour[Array[Int], Int]]](
//      edgeContext => {
//        edgeContext.sendToSrc(Seq(new JaBeJaNeighbour(edgeContext.attr, edgeContext.dstId, edgeContext.dstAttr)))
//        edgeContext.sendToDst(Seq(new JaBeJaNeighbour(edgeContext.attr, edgeContext.srcId, edgeContext.srcAttr)))
//      }, _ ++ _
//    )
//
//    val m3 = g1.vertices.innerJoin(m2)((vId, vd, nbh) => (vd, getRandomElems(nbh, config.nbhSetSize)))
//
//
//    val m4 = m3.mapValues((vId, data) => {
//      def isPartnerCorrect(colors: Array[Int], selfDominant: Int) = {
//        val partnerDominant = colors.zipWithIndex.maxBy(_._1)._2
//        colors.max < colors.sum && partnerDominant != selfDominant && colors(selfDominant) > 0
//      }
//
//      val (ownNbhColors, nbh) = data
//      val internal = ownNbhColors.max == ownNbhColors.sum
//
//      if (internal)
//        (None, None, None)
//      else {
//        val colorTuples = ownNbhColors.zipWithIndex
//        val minCard = colorTuples.minBy(_._1)._2
//        val dominant = colorTuples.maxBy(_._1)._2
//
//        val (selectedEdge, edgeColor) = getRandomElem(nbh.filter(_.edgeAttr == minCard)) match {
//          case Some(e) => (Some(e.nbhId), Some(e.edgeAttr))
//          case _       => (None, None)
//        }
//        val partner = nbh.find(n => isPartnerCorrect(n.nbtAttr, dominant)) match {
//          case Some(p) => Some(p.nbhId)
//          case _       => None
//        }
//        (selectedEdge, edgeColor, partner)
//      }
//    })
//
//    val g2 = g1.outerJoinVertices(m4)((vId, d, tr) => tr.getOrElse((None, None, None)))
//
//    val m5 = g2.aggregateMessages[List[Any]](
//      edgeContext => {
//
//      }, _ ++ _
//    )
//
//  }
//
//  def partitionByTriplets[VD](graph: Graph[VD, Int]) = {
//    val withEdges = collectEdges(graph)
//
//  }
//
//  def swap(graph: Graph[_, Int]) = {
//    val m1 = graph.aggregateMessages[Array[Int]](
//      edgeContext => {
//        val msg = buildArray(config.numOfPartition)(edgeContext.attr, 1)
//        edgeContext.sendToDst(msg)
//        edgeContext.sendToSrc(msg)
//      },
//      sumArrays(_, _)(_ + _),
//      TripletFields.EdgeOnly
//    )
//
//    val g1 = graph.outerJoinVertices(m1)((_, _, arr) => arr.getOrElse(Array.fill(config.numOfPartition)(0)))
//
//    val collectedEdges = g1.collectEdges(EdgeDirection.Either)
//    collectedEdges.persist()
//
//    val g1_2 = g1.vertices.innerJoin(collectedEdges)((vId, v, e) => {
//      val minCard = v.zipWithIndex.minBy(_._1)._2
//      getRandomElem(e.filter(ed => ed.attr == minCard))
//    })
//
//    val m2 = g1.aggregateMessages[(VertexId, Int, Int)](
//      edgeContext => {
//        val (_, srcDominant) = edgeContext.srcAttr.zipWithIndex.maxBy(_._1)
//        val (_, srcMinCard)  = edgeContext.srcAttr.zipWithIndex.minBy(_._1)
//        val (_, dstDominant) = edgeContext.dstAttr.zipWithIndex.maxBy(_._1)
//        val (_, dstMinCard)  = edgeContext.dstAttr.zipWithIndex.minBy(_._1)
//        val srcNotInternal = edgeContext.srcAttr.max < edgeContext.srcAttr.sum
//        val dstNotInternal = edgeContext.dstAttr.max < edgeContext.dstAttr.sum
//
//        if (srcDominant != dstDominant && srcNotInternal && dstNotInternal) {
//          if (edgeContext.srcAttr(dstDominant) > 0)
//            edgeContext.sendToSrc((edgeContext.dstId, dstDominant, dstMinCard))
//          if (edgeContext.dstAttr(srcDominant) > 0)
//            edgeContext.sendToDst((edgeContext.srcId, srcDominant, srcMinCard))
//        }
//
//      }, (x, y) => if (Random.nextBoolean()) x else y
//    )
//
//    val g2 = g1.outerJoinVertices(m2)((vId, nbh, msg) => (nbh, msg))
//
//    val g3 = g2.vertices.innerJoin(collectedEdges)((vId, data, edges) => {
//      val (nbh, selVertex) = data
//      val vertex = selVertex match {
//        case Some((_, selDom, _)) => getRandomElem(edges.filter(e => e.attr == selDom))
//        case _                    => None
//      }
//
//      selVertex match {
//        case Some((selVId, selDom, _)) => (Some(selVId), vertex)
//        case _                         => (None, None)
//      }
//    })
//
//  }
//
//  def swapColorHandshake(graph: Graph[(Array[Edge[Int]], Array[(VertexId, Array[Edge[Int]])]), Int]) = {
//    val g2 = graph.mapVertices({ case (vId, (edges, nbh)) => {
//      val selectedEdge = config.edgeSelectionPolicy.selectEdge(edges)
//      val selfDominant = getDominantColor(edges)
//      val partner = nbh.find({ case (vId, arr) => {
//        val partnerDominant = getDominantColor(arr)
//        partnerDominant != selfDominant &&  arr.exists(e => e.attr == selfDominant)
//      }})
//
//      partner match {
//        case Some(p) => (selectedEdge, Some(config.edgeSelectionPolicy.selectEdge(p._2, Some(selfDominant))))
//        case _       => (selectedEdge, None)
//      }
//    }})
//
//    val messages = g2.aggregateMessages[List[Edge[Int]]](
//      edgeContext => {
//        edgeContext.dstAttr match {
//          case (v1, Some(v2)) => {
//            if (v1.srcId == edgeContext.srcId || v1.dstId == edgeContext.srcId)
//              edgeContext.sendToSrc(List(v1))
//            if (v2.srcId == edgeContext.srcId || v2.dstId == edgeContext.srcId)
//              edgeContext.sendToSrc(List(v2))
//            if (v2.srcId == edgeContext.dstId || v2.dstId == edgeContext.dstId)
//              edgeContext.sendToDst(List(v2))
//          }
//          case _ =>
//        }
//
//        edgeContext.srcAttr match {
//          case (v1, Some(v2)) => {
//            if (v1.srcId == edgeContext.srcId || v1.dstId == edgeContext.srcId)
//              edgeContext.sendToSrc(List(v1))
//            if (v2.srcId == edgeContext.srcId || v2.dstId == edgeContext.srcId)
//              edgeContext.sendToSrc(List(v2))
//            if (v2.srcId == edgeContext.dstId || v2.dstId == edgeContext.dstId)
//              edgeContext.sendToDst(List(v2))
//          }
//          case _ =>
//        }
//      }, (x, y) => x ++ y
//    )
//
//    g2.outerJoinVertices(messages)((vId, v, msg) => {
//      v match {
//        case (e1, Some(e2)) => {
//          if (msg.getOrElse(List()).exists(e => edgeEquals(e, e1) || edgeEquals(e, e2)))
//            (None, None)
//          else
//            (Some(e1), Some(e2))
//        }
//      }
//
//    })
//  }
//
//  def partitionByNbhCollection[VD](graph: Graph[VD, Int]) = {
//    val withEdges = collectEdges(graph)
//    val withCandidates = collectCandidates(withEdges)
//
//
//  }
//
//  def collectEdges(graph: Graph[_, Int]) =
//    graph.outerJoinVertices(graph.collectEdges(EdgeDirection.Either))((_, _, l) => l.getOrElse(Array()))
//
//  def collectCandidates(graph: Graph[Array[Edge[Int]], Int]) = {
//    val messages = graph.aggregateMessages[List[(VertexId, Array[Edge[Int]])]](
//      edgeContext => {
//        edgeContext.sendToDst(List((edgeContext.srcId, edgeContext.srcAttr)))
//        edgeContext.sendToSrc(List((edgeContext.dstId, edgeContext.dstAttr)))
//      },
//      (x, y) => x ++ y
//    )
//
//    graph.outerJoinVertices(messages)({ case (vid, (e, ed), nbh) => (e, ed, nbh.getOrElse(List())) })
//  }
////    graph.outerJoinVertices(graph.collectNeighbors(EdgeDirection.Either))({ case (_, ed, nbh) =>
////      (ed, getRandomElems(nbh.getOrElse(Array()), config.nbhSetSize))
////    })
//
//
//  /*{
//       val groupedData = l.groupBy(e => e.attr)
//       val selectedEdge = JaBeJaUtils.getRandomElem(groupedData.minBy(t => t._2.size)._2, Random)
//       val dominantColor = groupedData.maxBy(t => t._2.size)._1
//       (dominantColor, selectedEdge)
//     })*/
//
//
//}
