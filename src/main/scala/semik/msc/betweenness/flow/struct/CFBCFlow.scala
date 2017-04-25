package semik.msc.betweenness.flow.struct

import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 4/23/17.
  */
class CFBCFlow(val src: VertexId, val dst: VertexId, val potential: Double, val completed: Boolean)

object CFBCFlow extends Serializable {
  def apply(src: VertexId,
            dst: VertexId,
            potential: Double = 1.0,
            completed: Boolean = false
           ): CFBCFlow = new CFBCFlow(src, dst, potential, completed)
}

object CFBCEmptyFlow extends Serializable {
  def apply(): CFBCFlow = new CFBCFlow(-1, -1, 0d, false)
}
