package semik.msc.experiments

/**
  * Created by mth on 5/30/17.
  */
object ExperimentsUtils {
  def textEdgeToTuple(edge: String) = {
    val vertices = edge.split("\t", 2)
    (vertices(0).toLong, vertices(1).toLong)
  }
}
