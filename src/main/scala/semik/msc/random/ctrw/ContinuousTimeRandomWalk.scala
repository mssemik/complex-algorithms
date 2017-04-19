package semik.msc.random.ctrw

import java.io.Serializable

import org.apache.spark.graphx.Pregel._
import org.apache.spark.graphx._
import semik.msc.factory.Factory
import semik.msc.neighbourhood.VertexNeighbourhood
import semik.msc.random.ctrw.factory.MessageFactory
import semik.msc.random.ctrw.processor.CTRWProcessor
import semik.msc.random.ctrw.struct.{CTRWMessage, CTRWVertex}
import semik.msc.tools.GraphSimplifier
import semik.msc.util.GraphSimplifier

import scala.util.Random

/**
  * Created by mth on 3/1/17.
  */
class ContinuousTimeRandomWalk[VD, ED](graph: Graph[VD, ED], initTemp: Double) extends Serializable {

  lazy val ctrwProcessor = new CTRWProcessor[VD, ED](graph, new MessageFactory(initTemp))

  def sampleVertices(sampleSize: Int = 1) = {

    def applyMessages(vertexId: VertexId, data: CTRWVertex, messages: List[CTRWMessage]) =
      if (data.initialized) ctrwProcessor.applyMessages(vertexId, data, messages) else ctrwProcessor.createInitMessages(data, sampleSize)

    val ctrwGraph = ctrwProcessor.initGraph

    val resultGraph = pregel(ctrwGraph, sampleSize)

//      ctrwGraph.ops.pregel(List.empty[CTRWMessage])(
//      applyMessages,
//      ctrwProcessor.sendMessage,
//      ctrwProcessor.mergeMessages
//    )

    val vertices = resultGraph.vertices

    val res = vertices.flatMap({ case (vertexId, data) => data.messages.map(s => (s.src, vertexId))})
      .aggregateByKey(List[VertexId]())((l, s) => l :+ s, _ ++ _)

    res.localCheckpoint
    res.count
    resultGraph.unpersistVertices(false)
    resultGraph.edges.unpersist(false)

    VertexRDD(res)
  }

  def pregel(graph: Graph[CTRWVertex, ED], sampleSize: Int) = {
    var g = graph.mapVertices((vid, vdata) => ctrwProcessor.createInitMessages(vdata, sampleSize))

    var messages = g.aggregateMessages(ctrwProcessor.sendMessageCtx, ctrwProcessor.mergeMessages)
    var activeMessages = messages.count()

    var prevG: Graph[CTRWVertex, ED] = null
    while (activeMessages > 0) {

      prevG = g
      g = g.ops.joinVertices(messages)(ctrwProcessor.applyMessages).cache()

      val oldMessages = messages

      messages = g.aggregateMessages(ctrwProcessor.sendMessageCtx, ctrwProcessor.mergeMessages).cache()

      activeMessages = messages.count()

      val k = messages.flatMap({ case (id, v) => v.map(m => m.temp) }).cache()

      val max = k.max()
      val min = k.min()
      val mean = k.mean()

      println(s"max: $max, min: $min, mean: $mean")

      k.unpersist(false)
      oldMessages.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)
    }
    messages.unpersist(blocking = false)
    g
  }

  def joinVertices(graph: Graph[CTRWVertex, ED], msg: VertexRDD[List[CTRWMessage]], mergeFun: (VertexId, CTRWVertex, List[CTRWMessage]) => CTRWVertex)(numOfIter: Int) = {
    def join = graph.ops.joinVertices(msg)(mergeFun)

    def rebuildGraphAndJoin = {
      graph.
    }

    if (numOfIter % 100 == 0) join else join
  }

}

