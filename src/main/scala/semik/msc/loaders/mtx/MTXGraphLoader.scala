package semik.msc.loaders.mtx

import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.graphx.{EdgeRDD, VertexRDD}
import semik.msc.loaders.GraphLoader
import semik.msc.parsers.mtx.MTXParser

/**
  * Created by mth on 12/5/16.
  */
class MTXGraphLoader extends GraphLoader {
  type graphValue = Map[String, Any]

  val parser = new MTXParser

  def loadDataFromFile(filePath : String)(implicit sc : SparkContext) = {
    val graphContent = sc.textFile(filePath)

//    val dataHeader = graphContent.take(2)
//
//    val graphHeader = parser.parseMetadata(dataHeader(0))
//    val graphDimensions = parser.parseGraphDimensions(dataHeader(1))
    val edges = graphContent.filter( s => !s.startsWith("%")).map(s => parser.parseEdge(s))

    val rddEdges = EdgeRDD.fromEdges[Map[String, Any], Long](edges)

    val rddVertex = VertexRDD.fromEdges(rddEdges, rddEdges.getNumPartitions, Map[String, Any]())

    (rddVertex, rddEdges)
  }


}
