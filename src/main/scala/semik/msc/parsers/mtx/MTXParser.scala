package semik.msc.parsers.mtx

import org.apache.spark.graphx.Edge
import semik.msc.parsers.mtx.syntax.{DataMetadata, GraphMetadata, MTXToken}

import scala.util.parsing.combinator.RegexParsers

/**
  * Created by mth on 12/5/16.
  */
class MTXParser extends RegexParsers with Serializable {

  def graphMetadataHeader = "%[ ]?MatrixMarket".r

  def objectMetadata = "(matrix)|(vector)".r

  def formatMetadata = "(coordinate)|(array)".r

  def fieldMetadata = "(real)|(double)|(complex)|(integer)|(pattern)".r

  def symmetryMetadata = "(general)|(symmetric)|(skew-symmetric)|(hermitian)".r

  def doubleMetadata = "[0-9\\.,-]+".r ^^ { case v => v.toDouble }

  def longMetadata = "[0-9]+".r ^^ { case v => v.toLong }

  def graphMetadata = graphMetadataHeader ~> objectMetadata ~ formatMetadata ~ fieldMetadata ~ symmetryMetadata ^^ {
    case obj ~ format ~ field ~ symmetry => new GraphMetadata(obj, format, field, symmetry)
  }

  def dimensionsMetadata = "[%]?".r ~> longMetadata ~ longMetadata ~ opt(longMetadata) ^^ { case n ~ m ~ nonZeros => new DataMetadata(n, m, nonZeros) }

  def edgeMetadata = longMetadata ~ longMetadata ~ opt(doubleMetadata) ^^ { case src ~ dest ~ weight => new Edge[Map[String, Any]](src, dest, Map("weight" -> weight)) }

  def parseDataLine = graphMetadata | edgeMetadata | dimensionsMetadata ^^ { case x => x }

  def parseEdge(edge: String) =
    parseAll(edgeMetadata, edge) match {
      case Success(x, _) => x
      case y => throw new Exception(y.toString)
    }

  def parseMetadata(metadata: String) =
    parseAll(graphMetadata, metadata) match {
      case Success(x, _) => x
      case y => throw new Exception(y.toString)
    }

  def parseGraphDimensions(dimensions: String) =
    parseAll(dimensionsMetadata, dimensions) match {
      case Success(x, _) => x
      case y => throw new Exception(y.toString)
    }
}