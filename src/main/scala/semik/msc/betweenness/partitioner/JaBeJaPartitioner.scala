//package semik.msc.betweenness.partitioner
//
//import org.apache.spark.graphx._
//import semik.msc.graph.JaBeJaVertex
//
//import scala.util.Random
//
///**
//  * Created by mth on 1/16/17.
//  */
//class JaBeJaPartitioner(val numPartitions: Int, initTemp: Double = 1.0, coldSpeed: Double = 0.0, alpha: Double = 1.0) extends Serializable {
//
//  type ColorTable = Array[Int]
//
//  var temperature: Double = initTemp
//
//  var halt = false
//
////  def repartition2[VD, ED](graph: Graph[VD, ED]) = {
////    val messages1 = graph.mapVertices((_, _) => new JaBeJaVertex(Random.nextInt(numPartitions)))
////      .aggregateMessages[ColorTable](
////        edgeCtx => {
////          val srcMsg = new ColorTable(numPartitions)
////          srcMsg(edgeCtx.dstAttr.color) += 1
////          edgeCtx.sendToSrc(srcMsg)
////
////          val dstMsg = new ColorTable(numPartitions)
////          dstMsg(edgeCtx.srcAttr.color) += 1
////          edgeCtx.sendToDst(dstMsg)
////        },
////        (x, y) => {
////          val out = x.clone()
////          for (i <- (0 to x.size-1))
////            out(i) += y(i)
////          out
////        }
////      )
////
////    val graph1 = graph.joinVertices(messages1)((vid, v, i) => v.setNeighbourColors(i))
////  }
//
//
//
//  def repartition[VD, ED](graph: Graph[VD, ED]) = {
//    val jaBeJaGraph = graph.mapVertices((_, data) => new JaBeJaVertex(-1))
//    var jaBeJaC = setInitColor(jaBeJaGraph)
//
////    initColoredGraph.mapVertices((vid, v) => v.color).vertices.groupBy({ case (_, c) => c }).map({ case (c, l) => (c, l.size) }).collect().sortBy({ case (k, _) => k }).foreach(println)
//    var prevG = jaBeJaC
//
//    while (!halt) {
//      prevG = jaBeJaC
//      jaBeJaC = colorChangeHandshake(findPartner(jaBeJaC))
//    }
//
//
////    f.mapVertices((vid, v) => v.color).vertices.groupBy({ case (_, c) => c }).map({ case (c, l) => (c, l.size) }).collect().sortBy({ case (k, _) => k }).foreach(println)
//
//    jaBeJaC
//  }
//
//  def setInitColor[VD, ED](graph: Graph[JaBeJaVertex, ED]) =
//    graph.mapVertices((_, vertex) => vertex.changeColor(Random.nextInt(numPartitions)))
//
//  def findPartner[VD, ED](graph: Graph[JaBeJaVertex, ED]) =
//    selectPartner(collectNeighbourColors(graph))
//
//  def collectNeighbourColors[VD, ED](graph: Graph[JaBeJaVertex, ED]) = {
//    def sendColor[VD, ED](edgeContext: EdgeContext[JaBeJaVertex, ED, Array[Int]]) = {
//      val msg = edgeContext.srcAttr.neighbourColors
//      msg(edgeContext.srcAttr.color) += 1
//      edgeContext.sendToDst(msg)
//
//      val msg1 = edgeContext.dstAttr.neighbourColors
//      msg1(edgeContext.dstAttr.color) += 1
//      edgeContext.sendToSrc(msg1)
//    }
//
//    def mergeColor(m1: Array[Int], m2: Array[Int]) = {
//      var i = 0
//      while (i < numPartitions) {
//        m1(i) += m2(i)
//        i += 1
//      }
//      m1
//    }
//
//    val messages = graph.mapVertices((vId, v) => v.setNeighbourColors(Array.fill[Int](numPartitions)(0)))
//      .aggregateMessages(sendColor _, mergeColor _)
//
////    println("Number of messages in collectNeighbourColors: " + messages.count())
//
//    graph.ops.joinVertices(messages)((vId, v, arr) => v.setNeighbourColors(arr))
//  }
//
//  def selectPartner[VD, ED](graph: Graph[JaBeJaVertex, ED]) = {
//
//    def sendMsg(edgeContext: EdgeContext[JaBeJaVertex, ED, (Option[VertexId], Double)]) = {
//      val oldValue = Math.pow(edgeContext.srcAttr.neighbourColors(edgeContext.srcAttr.color), alpha) +
//        Math.pow(edgeContext.dstAttr.neighbourColors(edgeContext.dstAttr.color), alpha)
//      val newValue = Math.pow(edgeContext.srcAttr.neighbourColors(edgeContext.dstAttr.color), alpha) +
//        Math.pow(edgeContext.dstAttr.neighbourColors(edgeContext.srcAttr.color), alpha)
//
//      if (newValue * temperature > oldValue) {
//        edgeContext.sendToDst((Some(edgeContext.srcId), newValue))
//        edgeContext.sendToSrc((Some(edgeContext.dstId), newValue))
//      }
//    }
//
//    def mergeMsg(x: (Option[VertexId], Double), y: (Option[VertexId], Double)) = if (x._2 > y._2) x else y
//
//    val messages = graph.aggregateMessages(sendMsg _, mergeMsg _)
//
////    println("Number of messages in selectPartner: " + messages.count())
//
//    graph.ops.joinVertices(messages)((vId, v, bp) => v.setBestNeighbour(bp._1, bp._2))
//  }
//
//  def colorChangeHandshake[VD, ED](graph: Graph[JaBeJaVertex, ED]) = {
//    def sendMsg[VD, ED](edgeContext: EdgeContext[JaBeJaVertex, ED, (Int, Double)]) = {
//      if (edgeContext.srcAttr.bestNeighbour.getOrElse(edgeContext.dstId) == edgeContext.dstId && edgeContext.dstAttr.bestNeighbour.getOrElse(edgeContext.srcId) == edgeContext.srcId) {
//        edgeContext.sendToDst((edgeContext.srcAttr.color, edgeContext.srcAttr.index))
//        edgeContext.sendToSrc((edgeContext.dstAttr.color, edgeContext.dstAttr.index))
//      }
//    }
//    val messages = graph.aggregateMessages[(Int, Double)](sendMsg _, (x, y) => if (x._2 > y._2) x else y)
//
//    val count = messages.countApprox(500, 0.8)
//    println("Number of messages in colorChangeHandshake: " + count)
//
//    if (count == 0)
//      halt = true
//
//    graph.ops.joinVertices(messages)((vId, v, c) => v.changeColor(c._1))
//  }
//
//}
