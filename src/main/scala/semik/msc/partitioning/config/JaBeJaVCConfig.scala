package semik.msc.partitioning.config

import org.apache.spark.graphx.Edge
import semik.msc.partitioning.policies.EdgeSelectionPolicy

/**
  * Created by mth on 1/28/17.
  */
class JaBeJaVCConfig(val numOfPartition: Int,
                     val nbhSetSize: Int = 10,
                     val edgeSelectionPolicy: EdgeSelectionPolicy[Int] = EdgeSelectionPolicy.greedy
                    ) extends Serializable
