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
      _ ++ _, 2, "NOBC - compute"
    )

//    val bcAggregator = new NearlyOptimalBCAggregator[ED](sigmaGraph)

    val bcvector = sigmaGraph.vertices.mapValues(v => v.bc / 2)

    normalizer.normalize(bcvector)
  }

}
