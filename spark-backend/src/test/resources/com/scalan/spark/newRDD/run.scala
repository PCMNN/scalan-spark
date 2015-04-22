package com.scalan.spark.newRDD

import org.apache.spark.{SparkConf, SparkContext}

object run {
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
  }

  private def exec(tuple: Int): Array[Int] = {
    try {
      new newRDD()(tuple)
    }
    finally stop()
  }

  def defaultValues() = values()

  def values() = { 20
  }
}


