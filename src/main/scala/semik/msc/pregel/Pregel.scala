package semik.msc.pregel

import org.apache.spark.graphx.{EdgeContext, Graph, VertexId, VertexRDD}
import semik.msc.random.ctrw.struct.{CTRWMessage, CTRWVertex}

import scala.reflect.ClassTag

/**
  * Created by mth on 4/19/17.
  */
object Pregel extends Serializable {

  def apply[OVD, VD: ClassTag, ED, MD: ClassTag](graph: Graph[OVD, ED],
                                       initMsg: (OVD) => VD,
                                       vpred: (VertexId, VD, MD) => VD,
                                       send: (EdgeContext[VD, ED, MD]) => Unit,
                                       merge: (MD, MD) => MD) = {

    def joinVertices(gr: Graph[VD, ED], msg: VertexRDD[MD], mergeFun: (VertexId, VD, MD) => VD)(numOfIter: Int) = {
      val resG = gr.ops.joinVertices(msg)(mergeFun)
      if (numOfIter % 50 == 0) { resG.checkpoint(); resG.vertices.count(); resG.edges.count(); }
      resG
    }

    var g = graph.mapVertices((vid, vdata) => initMsg(vdata))

    var messages = g.aggregateMessages(send, merge)
    var activeMessages = messages.count()

    var prevG: Graph[VD, ED] = null
    var i = 1
    while (activeMessages > 0) {

      prevG = g
      g = joinVertices(g, messages, vpred)(i).cache

      val oldMessages = messages

      messages = g.aggregateMessages(send, merge).cache

      activeMessages = messages.count()

      oldMessages.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)
      i = i+1
    }
    messages.unpersist(blocking = false)
    g
  }

}