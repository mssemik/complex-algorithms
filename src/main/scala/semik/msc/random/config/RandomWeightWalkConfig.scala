package semik.msc.random.config

/**
  * Created by mth on 2/6/17.
  */
class RandomWeightWalkConfig(val quantum: Double) extends Serializable {
  require(quantum < 1, "Quantum has to be less than 1")
}
