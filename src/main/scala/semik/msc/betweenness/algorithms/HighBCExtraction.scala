package semik.msc.betweenness.algorithms

import org.apache.spark.Partitioner
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD
import semik.msc.GraphPropTags
import semik.msc.loaders.GraphLoader

import scala.collection.convert.Wrappers.MutableMapWrapper
import scala.collection.mutable

/**
  * Created by mth on 12/13/16.
  */
class HighBCExtraction(graph: Graph[_, _], partitioner: Partitioner) {

  def prepareDataByPartitionMap = {
    val outgoingEdges = graph.edges.groupBy(_.srcId).mapPartitions(iter => iter.map(e => (e._1, Map(GraphPropTags.OutgoingEdges -> e._2.map(i => (i.dstId, i.attr)).toMap))))
    graph.vertices.fullOuterJoin(outgoingEdges).mapPartitions(iter => {
      iter.map({ case (id, (op1, op2)) => (id, op1.getOrElse(Map()).asInstanceOf[Map[String, _]] ++: op2.getOrElse(Map()).asInstanceOf[Map[String, _]]) })
    })
  }

  def prepareDataBySimpleMap = {
    val outgoingEdges = graph.edges.groupBy(_.srcId).map(e => (e._1, Map(GraphPropTags.OutgoingEdges -> e._2.map(i => (i.dstId, i.attr)).toMap)))
    graph.vertices.fullOuterJoin(outgoingEdges)
      .map({ case (id, (op1, op2)) => (id, op1.getOrElse(Map()).asInstanceOf[Map[String, _]] ++: op2.getOrElse(Map()).asInstanceOf[Map[String, _]]) })

  }

  def computeLocalVertices(graph: RDD[(VertexId, Map[String, Any])]) =
    graph.mapPartitions(v => {
      val vertexSet = v.map({ case (id, m) => id }).toSet

      v.map({ case (idx, map) => {
        (idx, map.updated(GraphPropTags.OutgoingEdges, map.get(GraphPropTags.OutgoingEdges).get.asInstanceOf[Map[Long, Map[String, Any]]].map({ case (inIdx, inMap) => (inIdx, inMap.updated(GraphPropTags.RemoteVertex, vertexSet.contains(inIdx))) })))
      }})
    })

}
