package semik.msc.random

/**
  * Created by mth on 2/7/17.
  */
object RWWMessageType extends Serializable {

  val none = 0x00

  val increment = 0x01

  val acknowledge = 0x02

  val nacknowledge = 0x03
}

object RWWVertexState extends Serializable {

  val ready = 0xA0

  val completed = 0xA3
}
