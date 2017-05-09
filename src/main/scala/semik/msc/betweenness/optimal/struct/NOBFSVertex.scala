package semik.msc.betweenness.optimal.struct

/**
  * Created by mth on 5/6/17.
  */
class NOBFSVertex(val startRound: Int, val distance: Double, val sigma: Int, val psi: Double) extends Serializable {
  def updateBC(p: Double) = NOBFSVertex(startRound, distance, sigma, psi + p)
}

object NOBFSVertex extends Serializable {
  def apply(startRound: Int, distance: Double, sigma: Int, psi: Double = .0): NOBFSVertex =
    new NOBFSVertex(startRound, distance, sigma, psi)
}
