package semik.msc.utils

/**
  * Created by mth on 1/26/17.
  */
object JaBeJaUtils {

  def buildArray[T](len: Int)(index: Int, value: T): Array[T] = {
    val array = Array.ofDim[T](len)
    array(index) = value
    array
  }
}
