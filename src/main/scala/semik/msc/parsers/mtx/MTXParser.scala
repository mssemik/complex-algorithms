package semik.msc.parsers.mtx

import semik.msc.parsers.mtx.syntax.{DataMetadata, EdgeMetadata, GraphMetadata, MTXToken}

import scala.util.parsing.combinator.RegexParsers

/**
  * Created by mth on 12/5/16.
  */
class MTXParser extends RegexParsers with Serializable {
  var metadata = Some()
  var dataSize = Some()

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

  def dimensionsMetadata(gm: GraphMetadata) = "[%]?".r ~> longMetadata ~ longMetadata ~ opt(longMetadata) ^^ { case n ~ m ~ nonZeros => new DataMetadata(n, m, nonZeros) }

  def edgeMetadata(gm: GraphMetadata) = longMetadata ~ longMetadata ~ opt(doubleMetadata) ^^ { case src ~ dest ~ weight => new EdgeMetadata(src, dest, weight)}

  def parseDataLine(gm: GraphMetadata) = graphMetadata | edgeMetadata(gm) | dimensionsMetadata(gm) ^^ { case x => x}

  def parseLine(edge: String, gm: GraphMetadata): MTXToken =
    parseAll(parseDataLine(gm), edge) match {
      case Success(x, _) => x
      case y => throw new Exception(y.toString)
    }
}