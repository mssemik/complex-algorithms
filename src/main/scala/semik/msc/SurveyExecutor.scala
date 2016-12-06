package semik.msc

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import semik.msc.loaders.mtx.MTXGraphLoader

/**
  * Created by mth on 12/5/16.
  */
object SurveyExecutor {

  def main(args : Array[String]) = {
    val sConf = new SparkConf(true).setAppName("complex-algorithms").setMaster("local")

    implicit val sc = new SparkContext(sConf)

    val session = SparkSession.builder().appName("complex-sql").master("local").getOrCreate()

    val sqlContext = session.sqlContext

    val parser = new MTXGraphLoader

    parser.loadDataFromFile("/media/mth/Data/repositories/Master Thesis code/soc-BlogCatalog.mtx")
  }
}
