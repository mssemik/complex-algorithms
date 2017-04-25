package semik.msc.betweenness.flow.struct

import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 4/23/17.
  */
class CFBCVertex(val id: VertexId, val degree: Int, val bc: Double, val sampleVertices: Array[VertexId], val flows: Array[CFBCFlow], val correlatedVertices: Array[VertexId]) extends Serializable {
  lazy val relatedFlows = flows.filter(f => f.dst == id || f.src == id)
  lazy val availableSamples = sampleVertices.diff(correlatedVertices)

  val vertexPhi = correlatedVertices.length
}

object CFBCVertex extends Serializable {
  def apply(id: VertexId,
            degree: Int,
            bc: Double = 0.0,
            sampleVertices: Array[VertexId] = Array.empty,
            flows: Array[CFBCFlow] = Array.empty,
            correlatedVertices: Array[VertexId] = Array.empty
           ): CFBCVertex = new CFBCVertex(id, degree, bc, sampleVertices, flows, correlatedVertices)
}
