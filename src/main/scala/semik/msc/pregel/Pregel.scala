package semik.msc.pregel

import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
  * Created by mth on 4/19/17.
  */
object Pregel extends Serializable {

  def apply[OVD, VD: ClassTag, ED, MD: ClassTag](graph: Graph[OVD, ED],
                                       prepareVertices: (OVD) => VD,
                                       vpred: (Int) => (VertexId, VD, Option[MD]) => VD,
                                       send: (Int) => (EdgeContext[VD, ED, MD]) => Unit,
                                       merge: (MD, MD) => MD,
                                       logF: (VertexRDD[MD]) => Unit) = {


    var round = 0

    var g = graph.mapVertices((vid, vdata) => prepareVertices(vdata))
    var messages = g.aggregateMessages(send(round), merge)
    var activeMessages = messages.count()
    logF(messages)

    var prevG: Graph[VD, ED] = null
    while (activeMessages > 0) {
      prevG = g
      g = g.outerJoinVertices(messages)(vpred(round)).cache
      g.vertices.localCheckpoint
      g.edges.localCheckpoint

      val oldMessages = messages
      round += 1

      messages = g.aggregateMessages(send(round), merge).cache

      activeMessages = messages.count()

      logF(messages)

      oldMessages.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)
    }
    messages.unpersist(blocking = false)
    g
  }

}
