package semik.msc.betweenness.flow.struct

import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 4/23/17.
  */
class CFBCFlow(val src: VertexId, val dst: VertexId, val potential: Double, val completed: Boolean) extends Serializable {
  def supplyValue(vertexId: VertexId) =
    if (src == vertexId) 1
    else if (dst == vertexId) -1
    else 0

  val key = (src, dst)
}

object CFBCFlow extends Serializable {
  def apply(src: VertexId,
            dst: VertexId,
            potential: Double = 1.0,
            completed: Boolean = false
           ): CFBCFlow = new CFBCFlow(src, dst, potential, completed)

  def updatePotential(flow: CFBCFlow, newPotential: Double, eps: Double = 0.0) = {
    val completed = Math.abs(flow.potential - newPotential) > eps
    CFBCFlow(flow.src, flow.dst, newPotential, completed)
  }

  def empty(src: VertexId, dst: VertexId) =
    CFBCFlow(src, dst, 0.0)
}

