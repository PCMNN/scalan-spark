package com.scalan.spark.SVDppSpark_errorMatrix

import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import com.scalan.spark.method.Methods._

object run {
  val globalSparkConf = new SparkConf().setAppName("SVDppSpark_errorMatrix").setMaster("local[8]")
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
    println("Result: " + v.map{ arr => arr.mkString(",")}.mkString(" , "))
  }

  def execute(): Unit = {
    exec(values())
  }

  def stop() = {
    globalSparkContext.stop()
  }

  private def exec(tuple: (ParametersPaired, (RDD[(Long,Array[Int])], (RDD[(Long,Array[Double])], (Int, Double))))) = {
    try {
      new SVDppSpark_errorMatrix()(tuple)
    }
    finally stop()
  }

  def defaultValues() = values()

  def values() = {
    globalSparkContext = new SparkContext(globalSparkConf)
    val params = (maxIterations, (delta, (gamma1, (gamma2, (lambda6, (lambda7, (width, stepDecrease)))))))
    val rddIdxs = globalSparkContext.makeRDD(jArrTrainIdxs.indices.map{_.toLong} zip jArrTrainIdxs, globalSparkContext.defaultParallelism)
    val rddIdxsPartitioned = (new PairRDDFunctions[Long, Array[Int]](rddIdxs)).partitionBy(defaultPartitioner(globalSparkContext.defaultParallelism))
    val rddVals = globalSparkContext.makeRDD(jArrTrainVals.indices.map{_.toLong} zip jArrTrainVals, globalSparkContext.defaultParallelism)
    val rddValsPartitioned = (new PairRDDFunctions[Long, Array[Double]](rddVals)).partitionBy(defaultPartitioner(globalSparkContext.defaultParallelism))
    (params, (rddIdxsPartitioned, (rddValsPartitioned, (nItems,stddev))))
  }
}

