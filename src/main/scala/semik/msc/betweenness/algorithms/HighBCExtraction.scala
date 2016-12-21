package semik.msc.betweenness.algorithms

import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD
import semik.msc.GraphPropTags
import semik.msc.betweenness.partitioner.HeuristicPartitioner
import semik.msc.graph.{Edge, Vertex}

/**
  * Created by mth on 12/13/16.
  */
class HighBCExtraction(graph: Graph[_, _], partitioner: HeuristicPartitioner) {

  def outgoingEdges = graph.edges.groupBy(_.srcId).mapPartitions(iter => iter.map(e => (e._1, Map(GraphPropTags.OutgoingEdges -> e._2.map(i => Edge(i.dstId)).toSeq))))

  def incomingEdges = graph.edges.groupBy(_.dstId).mapPartitions(iter => iter.map(e => (e._1, Map(GraphPropTags.IncomingEdges -> e._2.map(i => Edge(i.srcId)).toSeq))))

  def vertices = {
    incomingEdges.fullOuterJoin(outgoingEdges)
      .map({ case (id, (m1, m2)) =>
        (id, Vertex(id,
          m1.getOrElse(Map()).get(GraphPropTags.IncomingEdges).getOrElse(List()),
          m2.getOrElse(Map()).get(GraphPropTags.OutgoingEdges).getOrElse(List())
        ))
      })
  }

  def repartition = partitioner.computePartitions(vertices)

  def computeLocalVertices(graph: RDD[(VertexId, Map[String, Any])]) =
    graph.mapPartitions(v => {
      val vertexSet = v.map({ case (id, m) => id }).toSet

      v.map({ case (idx, map) => {
        (idx, map.updated(GraphPropTags.OutgoingEdges, map.get(GraphPropTags.OutgoingEdges).get.asInstanceOf[Map[Long, Map[String, Any]]].map({ case (inIdx, inMap) => (inIdx, inMap.updated(GraphPropTags.RemoteVertex, vertexSet.contains(inIdx))) })))
      }
      })
    })

}
