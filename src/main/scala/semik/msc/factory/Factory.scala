package semik.msc.factory

/**
  * Created by mth on 4/13/17.
  */
trait Factory[A, R] extends Serializable {
  def create(arg: A): R
  def correct(arg: A, curr: R): R
}
