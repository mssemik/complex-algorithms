package semik.msc.betweenness.algorithms

import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader

/**
  * Created by mth on 1/8/17.
  */
class StreamPartitioningLoader {

  def loadDataFromFile(filePath: String)(implicit sc: SparkContext) = {
    val graph = GraphLoader.edgeListFile(sc, filePath, true, 50)

//    graph.aggregateMessagesWithActiveSet()
  }
}
