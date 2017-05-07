package semik.msc.betweenness.optimal

import org.apache.spark.graphx.{Graph, VertexId}
import semik.msc.betweenness.optimal.processor.NearlyOptimalBCProcessor
import semik.msc.betweenness.optimal.struct.messages.{DFSPointer, NOMessage}
import semik.msc.betweenness.optimal.struct.{NOBFSVertex, NOVertex}
import semik.msc.pregel.Pregel

import scala.reflect.ClassTag

/**
  * Created by mth on 5/6/17.
  */
class NearlyOptimalBC[VD, ED: ClassTag](graph: Graph[VD, ED]) extends Serializable {

  private val nOBCProcessor = new NearlyOptimalBCProcessor[VD, ED](graph)

  def computeBC = {
    val initBFSGraph = nOBCProcessor.initGraph

    Pregel[NOVertex, NOVertex, ED, List[NOMessage[VertexId]]](initBFSGraph,
      nOBCProcessor.prepareVertices(nOBCProcessor.initVertexId),
      nOBCProcessor.applyMessages,
      nOBCProcessor.sendMessages,
      _ ++ _, v => {
        val k = v.flatMap({ case (id, vv) => vv.map(j => (id, j)) }).filter(_._2.isDFSPointer).collect().map(j => (j._1, j._2.asInstanceOf[DFSPointer]))
        k.foreach(j => println(s"${j._1} -> ${j._2.next.getOrElse(-1)}, toSend: ${j._2.toSent}"))
        println("###############")
      }
    )
  }


}
