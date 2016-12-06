package semik.msc.loaders.mtx

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import semik.msc.loaders.GraphLoader
import semik.msc.parsers.mtx.MTXParser

import scala.util.parsing.combinator.RegexParsers

/**
  * Created by mth on 12/5/16.
  */
class MTXGraphLoader extends GraphLoader {
  type graphValue = Map[String, Any]

  def loadDataFromFile(filePath : String)(implicit sc : SparkContext) = {

    val graphContent = sc.textFile(filePath)

    val rawData = graphContent.filter(s => !s.startsWith("%"))

    val parser = new MTXParser

    val edges = rawData.map(s => parser.parseEdge(s))

    edges.foreach(e => println("From "+e._1+" to "+e._2))
  }


}
