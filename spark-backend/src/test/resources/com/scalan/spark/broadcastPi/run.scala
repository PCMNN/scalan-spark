package com.scalan.spark.broadcastPi

import org.apache.spark.{SparkConf, SparkContext}

object run {
  val globalSparkConf = new SparkConf().setAppName("R/W Broadcast").setMaster("local[4]")
  var globalSparkContext: SparkContext = null

  def main(args: Array[String]): Unit = {
    println("Start")
    val v = exec(defaultValues())
    println(s"Results: $v")
    println("Done")
  }

//  def execute(): Unit = {
//    exec(defaultValues())
//  }

  def stop() = {
    globalSparkContext.stop()
  }

  private def exec(tuple: SparkContext): Double = {
    try {
      val sparkData: org.apache.spark.broadcast.Broadcast[Double] = new broadcastPi()(tuple)
      val scalaData = sparkData.value
      scalaData
    }
    finally stop()
  }

  def defaultValues() = values()

  def values() = {
    globalSparkContext = new SparkContext(globalSparkConf)
    (globalSparkContext)
  }
}

