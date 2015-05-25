package com.scalan.spark.transposeFun

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object run {
 val globalSparkConf = new SparkConf().setAppName("R/W Broadcast").setMaster("local[8]")
 var globalSparkContext: SparkContext = null

 lazy val jArrTrainIdxs = Array(Array(0,2), Array(0), Array(2,3))
 lazy val jArrTrainVals = Array(Array(1.0, 1.2), Array(2.0), Array(3.2, 3.2))
 lazy val nItems = 4



 def main(args: Array[String]): Unit = {
  println("Start")
  val v = exec(defaultValues())
  println(v(0).mkString(" , "))
  println(v(1).mkString(" , "))
  println(v(2).mkString(" , "))
  println(v(3).mkString(" , "))
  println("Done")
 }

 def execute(): Unit = {
  exec(values())
 }

 def stop() = {
  globalSparkContext.stop()
 }

 private def exec(tuple: (RDD[Array[Int]], (RDD[Array[Double]], Int))) = {
  try {
   new transposeFun()(tuple)
  }
  finally stop()
 }

 def defaultValues() = values()

 def values() = {
  globalSparkContext = new SparkContext(globalSparkConf)
  val rddIdxs = globalSparkContext.makeRDD(jArrTrainIdxs)
  val rddVals = globalSparkContext.makeRDD(jArrTrainVals)
  (rddIdxs, (rddVals, nItems))
 }

}