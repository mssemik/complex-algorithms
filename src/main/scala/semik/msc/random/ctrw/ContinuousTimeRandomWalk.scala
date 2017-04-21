package semik.msc.random.ctrw

import java.io.Serializable

import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import semik.msc.pregel.Pregel
import semik.msc.random.ctrw.factory.MessageFactory
import semik.msc.random.ctrw.processor.CTRWProcessor
import semik.msc.random.ctrw.struct.{CTRWMessage, CTRWVertex}

/**
  * Created by mth on 3/1/17.
  */
class ContinuousTimeRandomWalk[VD, ED](graph: Graph[VD, ED], initTemp: Double) extends Serializable {

  lazy val ctrwProcessor = new CTRWProcessor[VD, ED](graph, new MessageFactory(initTemp))

  def sampleVertices(sampleSize: Int = 1) = {

    val resultGraph = Pregel[CTRWVertex, CTRWVertex, ED, List[CTRWMessage]](
      ctrwProcessor.initGraph,
      ctrwProcessor.createInitMessages(sampleSize),
      ctrwProcessor.applyMessages,
      ctrwProcessor.sendMessageCtx,
      ctrwProcessor.mergeMessages, log)

    val vertices = resultGraph.vertices

    val res = vertices.flatMap({ case (vertexId, data) => data.messages.map(s => (s.src, vertexId))})
      .aggregateByKey(List[VertexId]())((l, s) => l :+ s, _ ++ _)

    res.localCheckpoint
    res.count
    resultGraph.unpersistVertices(false)
    resultGraph.edges.unpersist(false)

    VertexRDD(res)
  }

  def log(v: VertexRDD[List[CTRWMessage]]) = println(s"numOfMsg: ${v.aggregate(0)({ case (l, (id, v)) => l + v.size }, _ + _)}")
}

