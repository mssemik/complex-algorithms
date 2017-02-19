package semik.msc.loaders.mtx

import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.graphx._
import semik.msc.loaders.GraphLoader
import semik.msc.parsers.mtx.MTXParser

import scala.collection.convert.Wrappers.MutableMapWrapper
import scala.collection.mutable

/**
  * Created by mth on 12/5/16.
  */
class MTXGraphLoader extends GraphLoader {

  val parser = new MTXParser

  def loadDataFromFile(filePath: String)(implicit sc: SparkContext) = {

    GraphLoader.edgeListFile(sc, filePath, true, 10)//.mapVertices((_, _) => Map[String, Any]()).mapEdges(_ => Map[String, Any]())

//    val graphContent = sc.textFile(filePath, 50)
//
//    val edges = graphContent.mapPartitions(iter => {
//      val res = iter.map(_.split(" "))
//      val res2 = res.map(parseEdge)
//      res2
//    })

//    val edgesRdd = EdgeRDD.fromEdges[Map[String, Any], Map[String, Any]](edges)
//
//    val vertex = VertexRDD.fromEdges[_](edgesRdd, edgesRdd.getNumPartitions, Map[String, Any]())

//    Graph.fromEdges(edges, prepareGraphProperties)
  }

  def parseEdge(edge: Array[String]): Edge[graphProperties] = edge match {
    case Array(s: String, d: String, w: String) => new Edge[graphProperties](s.toLong, d.toLong, Map("weight" -> w.toDouble))
    case Array(s: String, d: String) => new Edge[graphProperties](s.toLong, d.toLong, Map())
  }

  def prepareGraphProperties: graphProperties = Map()
  def prepareGraphProperties(elems: (String, Any)): graphProperties = Map[String, Any](elems)


}
