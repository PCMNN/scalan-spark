package com.scalan.spark.SVDppSpark_Train

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import com.scalan.spark.method.Methods._

object run {
  val globalSparkConf = new SparkConf().setAppName("R/W Broadcast").setMaster("local[8]")
  var globalSparkContext: SparkContext = null

  type ParametersPaired = (Int, (Double, (Double, (Double, (Double, (Double, (Int, Double)))))))
  lazy val maxIterations = 12
  lazy val delta = 1e-6
  lazy val gamma1 = 1e-3
  lazy val gamma2 = 1e-3
  lazy val lambda6 = 1e-2
  lazy val lambda7 = 1e-2
  lazy val stepDecrease = 0.99
  lazy val jArrTrainIdxs = Array(Array(0,1), Array(1))
  lazy val jArrTrainVals = Array(Array(5.0, 3.0), Array(4.0))
  lazy val nItems = 2
  lazy val width = 5
  lazy val stddev = 0.03



  def main(args: Array[String]): Unit = {
    println("Start")
    val v = exec(defaultValues())
    println("Result1: " + v._1.mkString(" , "))
    println("Result2: " + v._2._1.mkString(" , "))
    println("Result3: " + v._2._2._1)
    println("Result4: " + v._2._2._2._1)
    println("Result5: " + v._2._2._2._2._1)
    println("Result6: " + v._2._2._2._2._2)
    println("Done")
  }

  def execute(): Unit = {
    exec(values())
  }

  def stop() = {
    globalSparkContext.stop()
  }

  private def exec(tuple: (ParametersPaired, (RDD[Array[Int]], (RDD[Array[Double]], (Int, Double))))) = {
    try {
      new SVDppSpark_Train()(tuple)
    }
    finally stop()
  }

  def defaultValues() = values()

  def values() = {
    globalSparkContext = new SparkContext(globalSparkConf)
    val params = (maxIterations, (delta, (gamma1, (gamma2, (lambda6, (lambda7, (width, stepDecrease)))))))
    val rddIdxs = globalSparkContext.makeRDD(jArrTrainIdxs, globalSparkContext.defaultParallelism)
    val rddIdxsPartitioned = partitionBy(rddIdxs, defaultPartitioner(globalSparkContext.defaultParallelism))
    val rddVals = globalSparkContext.makeRDD(jArrTrainVals, globalSparkContext.defaultParallelism)
    val rddValsPartitioned = partitionBy(rddVals, defaultPartitioner(globalSparkContext.defaultParallelism))
    (params, (rddIdxsPartitioned, (rddValsPartitioned, (nItems,stddev))))
  }
}

