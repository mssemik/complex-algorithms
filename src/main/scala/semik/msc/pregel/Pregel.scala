package semik.msc.pregel

import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
  * Created by mth on 4/19/17.
  */
object Pregel extends Serializable {

  def apply[OVD, VD: ClassTag, ED, MD: ClassTag](graph: Graph[OVD, ED],
                                       prepareVertices: (OVD) => VD,
                                       vpred: (VertexId, VD, Option[MD]) => VD,
                                       send: (EdgeContext[VD, ED, MD]) => Unit,
                                       merge: (MD, MD) => MD,
                                       logF: (VertexRDD[MD]) => Unit) = {

    def joinVertices(gr: Graph[VD, ED], msg: VertexRDD[MD], mergeFun: (VertexId, VD, Option[MD]) => VD)(numOfIter: Int) = {
      val resG = gr.outerJoinVertices(msg)(mergeFun)
      if (numOfIter % 50 == 0) { resG.checkpoint(); resG.vertices.count(); resG.edges.count(); }
      resG
    }

    var g = graph.mapVertices((vid, vdata) => prepareVertices(vdata))

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

      logF(messages)

      oldMessages.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)
      i = i+1
    }
    messages.unpersist(blocking = false)
    g
  }

}
