package com.scalan.spark.broadcastPi

import org.apache.spark.{SparkConf, SparkContext}

object run {
  val globalSparkConf = new SparkConf().setAppName("R/W Broadcast").setMaster("local[8]")
  var globalSparkContext: SparkContext = null

  def main(args: Array[String]): Unit = {
    println("Start")
    val v = exec(defaultValues())
    println(s"Results: $v")
    println("Done")
  }

  def execute(): Unit = {
    exec(values())
  }

  def stop() = {
    globalSparkContext.stop()
  }

  private def exec(tuple: SparkContext): org.apache.spark.broadcast.Broadcast[Double] = {
    try {
      new broadcastPi()(tuple)
    }
    finally stop()
  }

  def defaultValues() = values()

  def values() = {
    globalSparkContext = new SparkContext(globalSparkConf)
    (globalSparkContext)
  }
}

