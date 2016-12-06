package semik.msc.parsers.mtx.syntax

/**
  * Created by mth on 12/6/16.
  */
class EdgeMetadata(val source: Long, val destination: Long, val weight: Option[Double]) extends MTXToken {
  override def toString: String = "From "+source+" to "+destination+" of weight "+weight.getOrElse(1)
}
