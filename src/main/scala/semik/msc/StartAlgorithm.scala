package semik.msc

import java.nio.file.Files

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.spark_project.guava.io
import semik.msc.betweenness.edmonds.EdmondsBC
import semik.msc.betweenness.flow.CurrentFlowBC
import semik.msc.betweenness.flow.factory.FlowFactory
import semik.msc.betweenness.flow.generator.impl.RandomFlowGenerator
import semik.msc.betweenness.optimal.NearlyOptimalBC
import semik.msc.bfs.BFSShortestPath
import semik.msc.bfs.predicate.BFSVertexPredicate
import semik.msc.bfs.processor.BFSProcessor
import semik.msc.experiments.ExperimentManager
import semik.msc.random.ctrw.ContinuousTimeRandomWalk

import scala.io.Source

/**
  * Created by mth on 3/15/17.
  */
object StartAlgorithm {

  def main(args: Array[String]): Unit = {

    if (args.length < 4)
      throw new Error("Za mało paramsów")

    val mode = args(0) match {
      case "E" | "F" | "O" => args(0)
      case _ => throw new Error("błedny tryb")
    }

    val file = args(1)
    val numOfPartition = args(2).toInt
    val iteration = args(3).toInt

    mode match {
      case "E" => ExperimentManager().runEdmondsAlgorithm(file, numOfPartition, iteration)
      case "F" => ExperimentManager().runCurrentFlowAlgorithm(args(4).toInt, args(5).toInt, args(6).toDouble)(file, numOfPartition, iteration)
      case "O" => ExperimentManager().runOptimalAlgorithm(file, numOfPartition, iteration)
    }
  }
}
