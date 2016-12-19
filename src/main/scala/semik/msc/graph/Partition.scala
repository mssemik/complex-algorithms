package semik.msc.graph

/**
  * Created by mth on 12/18/16.
  */
class Partition[ID, VD](val elems: Map[ID, VD]) extends Serializable {

  def this(elemsList: List[Tuple2[ID, VD]]) = this(elemsList.toMap)

  def elem(key: ID) = elems.get(key)

  def size = elems.size
}
