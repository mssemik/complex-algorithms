package semik.msc.bfs

import java.util.Date

import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.Encoders
import semik.msc.predicate.vertex.VertexPredicate
import semik.msc.processor.PregelProcessor

import scala.reflect.ClassTag

/**
  * Created by mth on 3/13/17.
  */
class BFSShortestPath[VD: ClassTag, ED, MD: ClassTag](vPredicate: VertexPredicate[VD, MD], processor: PregelProcessor[VD, ED, MD]) extends Serializable {

  def computeSingleSelectedSourceBFS(graph: Graph[VD, ED], source: VertexId, undirected: Boolean = true): Graph[VD, ED] = {
    val startTime = System.nanoTime()

    val initGraph = graph.mapVertices((vId, attr) => vPredicate.getInitialData(vId, attr)(source)).cache

    val result = initGraph.ops.pregel[MD](processor.initialMessage)(
      vPredicate.applyMessages,
      processor.sendMessage(_, bidirectional = undirected),
      processor.mergeMessages
    )

    val finishTime = System.nanoTime()
    println("Time of execution computeSingleSelectedSourceBFS:" + ((finishTime - startTime)/1000000) + " ms")

    initGraph.unpersist(false)
    result
  }


}
