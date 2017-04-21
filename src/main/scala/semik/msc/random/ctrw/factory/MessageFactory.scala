package semik.msc.random.ctrw.factory

import semik.msc.factory.Factory
import semik.msc.random.ctrw.struct.{CTRWMessage, CTRWVertex}

import scala.util.Random

/**
  * Created by mth on 4/13/17.
  */
class MessageFactory(temp: Double) extends Serializable with Factory[CTRWVertex, CTRWMessage] {
  override def create(vertex: CTRWVertex): CTRWMessage = {
    val sampleVertex = takeRandomNeighbour(vertex)
    CTRWMessage(vertex.id, temp, sampleVertex)
  }

  override def correct(vertex: CTRWVertex, message: CTRWMessage): CTRWMessage = {
    val diff = Math.log(Random.nextDouble()) / vertex.degree
    val newTemp = message.temp + diff
    val nextVertex = if (newTemp > 0) takeRandomNeighbour(vertex) else None
    CTRWMessage(message.src, newTemp, nextVertex)
  }

  private def takeRandomNeighbour(vertex: CTRWVertex) =
    if (vertex.degree > 0) Some(vertex.neighbours(Random.nextInt(vertex.degree))) else None
}
