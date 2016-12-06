package semik.msc.parsers.mtx.syntax

/**
  * Created by mth on 12/6/16.
  */
class DataMetadata(val m: Long, val n: Long, val nonZeros: Option[Long]) extends MTXToken {
  override def toString: String = "MxN: "+m+"x"+n+", nonZeros: "+nonZeros.getOrElse("undefined")
}
