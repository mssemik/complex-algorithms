package semik.msc.parsers.mtx.syntax

/**
  * Created by mth on 12/6/16.
  */
class GraphMetadata(val obj: String, val format: String, val field: String, val symmetry: String) extends MTXToken {
  override def toString: String = "Metadata: "+obj+", "+format+", "+field+", "+symmetry
}
