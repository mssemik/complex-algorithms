package semik.msc.parsers.mtx

import scala.util.parsing.combinator.RegexParsers

/**
  * Created by mth on 12/5/16.
  */
class MTXParser extends RegexParsers with Serializable {
  val vertexId = "[0-9]+".r

  def parse: Parser[(Long, Long)] =
    vertexId ~ vertexId ^^ { case src ~ dest => new Tuple2(src.toLong, dest.toLong)}

  def parseEdge(edge: String) : (Long, Long) =
    parseAll(parse, edge) match {
      case Success(x, _) => x
      case y => throw new Exception(y.toString)
    }
}
