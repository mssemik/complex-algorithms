package semik.msc.random.ctrw

import java.io.Serializable

import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import semik.msc.pregel.Pregel
import semik.msc.random.ctrw.factory.MessageFactory
import semik.msc.random.ctrw.processor.CTRWProcessor
import semik.msc.random.ctrw.struct.{CTRWMessage, CTRWVertex}

import scala.reflect.ClassTag

/**
  * Created by mth on 3/1/17.
  */
class ContinuousTimeRandomWalk[VD, ED: ClassTag](graph: Graph[VD, ED], initTemp: Double = 2.3) extends Serializable {

  lazy val ctrwProcessor = new CTRWProcessor[VD, ED](graph, new MessageFactory(initTemp))

  def sampleVertices(sampleSize: Int = 1) = {

    val resultGraph = Pregel[CTRWVertex, CTRWVertex, ED, List[CTRWMessage]](
      ctrwProcessor.initGraph,
      ctrwProcessor.createInitMessages(sampleSize),
      ctrwProcessor.applyMessages,
      ctrwProcessor.sendMessageCtx,
      ctrwProcessor.mergeMessages, opName = "CTRW")

    val vertices = resultGraph.mapVertices((id, v) => v).vertices

    val res = vertices.flatMap({ case (vertexId, data) => data.messages.map(s => (s.src, vertexId))})
      .aggregateByKey(List[VertexId]())((l, s) => l :+ s, _ ++ _)

    res.checkpoint()
    res.count
    resultGraph.unpersistVertices(false)
    resultGraph.edges.unpersist(false)

    VertexRDD(res)
  }

}

