/**
 * Created by afilippov on 4/29/15.
 */
package com.scalan.spark.factories_BL_TrainAndTest

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object run {
  val globalSparkConf = new SparkConf().setAppName("R/W Broadcast").setMaster("local[8]")
  var globalSparkContext: SparkContext = null

  type ParametersBL = (Int, (Double, (Double, (Double, Double))))
  lazy val maxIterations = 12
  lazy val delta = 1e-6
  lazy val gamma1 = 1e-3
  lazy val lambda6 = 1e-2
  lazy val stepDecrease = 0.99
  lazy val jArrTrainIdxs = Array(Array(0,1), Array(1))
  lazy val jArrTrainVals = Array(Array(5.0, 3.0), Array(4.0))
  lazy val jArrTestIdxs = Array(Array(0,1), Array(0,1))
  lazy val jArrTestVals = Array(Array(5.0, 3.0), Array(3.0, 4.0))
  lazy val nItems = 2



  def main(args: Array[String]): Unit = {
    println("Start")
    val v = exec(defaultValues())
    println("Result: " + v)
    println("Done")
  }

  def execute(): Unit = {
    exec(values())
  }

  def stop() = {
    globalSparkContext.stop()
  }

  private def exec(tuple: (ParametersBL, (RDD[Array[Int]], (RDD[Array[Double]], (RDD[Array[Int]], (RDD[Array[Double]], Int)))))) = {
    try {
      new factories_BL_TrainAndTest()(tuple)
    }
    finally stop()
  }

  def defaultValues() = values()

  def values() = {
    globalSparkContext = new SparkContext(globalSparkConf)
    val params = (maxIterations, (delta, (gamma1, (lambda6, stepDecrease))))
    val rddTrainIdxs = globalSparkContext.makeRDD(jArrTrainIdxs)
    val rddTrainVals = globalSparkContext.makeRDD(jArrTrainVals)
    val rddTestIdxs = globalSparkContext.makeRDD(jArrTrainIdxs)
    val rddTestVals = globalSparkContext.makeRDD(jArrTrainVals)
    (params, (rddTrainIdxs, (rddTrainVals, (rddTestIdxs, (rddTestVals,nItems)))))
  }
}
