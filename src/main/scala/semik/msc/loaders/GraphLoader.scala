package semik.msc.loaders

import scala.collection.mutable

/**
  * Created by mth on 12/5/16.
  */
trait GraphLoader extends Serializable {
  type graphProperties = Map[String, Any]
}
