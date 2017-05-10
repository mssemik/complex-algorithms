package semik.msc.betweenness.optimal

import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import semik.msc.betweenness.normalizer.BCNormalizer
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
  val normalizer = new BCNormalizer(graph)

  def computeBC = {
    val initBFSGraph = nOBCProcessor.initGraph

    val sigmaGraph = Pregel[NOVertex, NOVertex, ED, List[NOMessage[VertexId]]](initBFSGraph,
      nOBCProcessor.prepareVertices(nOBCProcessor.initVertexId),
      nOBCProcessor.applyMessages,
      nOBCProcessor.sendMessages,
      _ ++ _, logMessages(_), 2
    )

    sigmaGraph.vertices.foreach({ case (id, v) => v.bfsMap.foreach({ case (src, b) => println(s"$id -> $src = ${b.startRound}")})})

    val bcAggregator = new NearlyOptimalBCAggregator[ED](sigmaGraph)

    val bcvector = bcAggregator.aggragateBC

    normalizer.normalize(bcvector)
  }

  def logMessages(messages: VertexRDD[List[NOMessage[VertexId]]]) = None //{
//    val dfs = messages.flatMap({ case (k, l) => l.map(i => (k, i))}).filter(_._2.isDFSPointer).collect()
//    dfs.foreach({ case (d, p: DFSPointer) => println(s"$d :: ${p.next.getOrElse(-1)} : ${p.toSent}") })
//    println("##################")
//  }


}
