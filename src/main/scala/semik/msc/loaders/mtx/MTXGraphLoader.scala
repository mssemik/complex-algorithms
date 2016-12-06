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
class MTXGraphLoader extends GraphLoader with Serializable {
  type graphValue = Map[String, Any]

  val parser = new MTXParser

  def loadDataFromFile(filePath : String)(implicit sc : SparkContext) = {
    val graphContent = sc.textFile(filePath)

    val edges = graphContent.map(s => parser.parseLine(s, null))

    edges.take(500).foreach(println)
  }


}
