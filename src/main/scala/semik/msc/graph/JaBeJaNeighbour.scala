package semik.msc.graph

import org.apache.spark.graphx.VertexId
/**
  * Created by mth on 2/2/17.
  */
class JaBeJaNeighbour[ND, @specialized ED] (
    val edgeAttr: ED,
    val nbhId: VertexId,
    val nbtAttr: ND) extends Serializable {

}
