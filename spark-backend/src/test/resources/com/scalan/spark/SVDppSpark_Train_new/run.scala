package com.scalan.spark.SVDppSpark_Train_new

import java.io.File

import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import com.scalan.spark.method.Methods._

import scala.io.Source

object run {
  val globalSparkConf = new SparkConf().setAppName("R/W Broadcast").setMaster("local[4]")
  /*val globalSparkConf = new SparkConf().setAppName("R/W Broadcast").
     //setMaster("local[2]").
     setMaster("spark://10.122.85.159:7078").
     setJars(Array("SVDppSpark_Train_new.jar")).
     set("spark.executor.extraJavaOptions",
       "-XX:+UseG1GC -XX:ParallelGCThreads=12 -XX:ConcGCThreads=5 -XX:+UseCompressedOops").
     set("spark.executor.memory", "2g").set("spark.cores.max", "48")
  */
  var globalSparkContext: SparkContext = null

  type ParametersPaired = (Int, (Double, (Double, (Double, (Double, (Double, (Int, Double)))))))
  lazy val maxIterations = 12
  lazy val delta = 1e-6
  lazy val gamma1 = 1e-3
  lazy val gamma2 = 1e-3
  lazy val lambda6 = 1e-2
  lazy val lambda7 = 1e-2
  lazy val stepDecrease = 0.99
  lazy val width = 5
  lazy val stddev = 1e-5 //0.03
  val (_, arrTrainIndices, arrTrainValues) = LoadMovieLensData_IntoRatingMatrix(0.0, 1.0, ml.ub.base, -1.0, ml.nUsers, ml.nItems)


  def main(args: Array[String]): Unit = {
    println("Start")
    val v = exec(defaultValues())
    println("Result: " + v) //.map { arr => arr.mkString(",")}.mkString(" , "))
  }

  def execute(): Unit = {
    exec(values())
  }

  def stop() = {
    globalSparkContext.stop()
  }

  private def exec(tuple: (ParametersPaired, (RDD[(Long, Array[Int])], (RDD[(Long, Array[Double])], (Int, Double))))) = {
    try {
      val from = scala.compat.Platform.currentTime
      new SVDppSpark_Train_new()(tuple)
      val to = scala.compat.Platform.currentTime
      println(s"SVDppSpark_Train_new().apply(tuple) spend ${(to-from)/1000.0} ms")
    }
    finally stop()
  }

  def defaultValues() = values()

  def values() = {
    globalSparkContext = new SparkContext(globalSparkConf)
    val params = (maxIterations, (delta, (gamma1, (gamma2, (lambda6, (lambda7, (width, stepDecrease)))))))

    val rddIdxs = globalSparkContext.makeRDD(arrTrainIndices.indices.map { _.toLong} zip arrTrainIndices, globalSparkContext.defaultParallelism)
    val rddIdxsPartitioned = (new PairRDDFunctions[Long, Array[Int]](rddIdxs)).partitionBy(defaultPartitioner(globalSparkContext.defaultParallelism))
    val rddVals = globalSparkContext.makeRDD(arrTrainValues.indices.map {_.toLong} zip arrTrainValues, globalSparkContext.defaultParallelism)
    val rddValsPartitioned = (new PairRDDFunctions[Long, Array[Double]](rddVals)).partitionBy(defaultPartitioner(globalSparkContext.defaultParallelism))
    (params, (rddIdxsPartitioned, (rddValsPartitioned, (ml.nItems, stddev))))
  }

  def LoadMovieLensData_IntoRatingMatrix(start: Double, end: Double, rawDataFile: File, outputMinValue: Double, maxUsers: Int, maxItems: Int):
  (Int, Array[Array[Int]], Array[Array[Double]]) = {
    val ifs = Source.fromFile(rawDataFile)
    val lengthItems: Array[Int] = new Array[Int](maxUsers)
    for (line1 <- ifs.getLines()) {
      val line = line1.trim()
      val tokens = line.split("\t")
      val user = tokens(0).toInt - 1
      if ((user >= 0) && (user < maxUsers)) lengthItems(user) = lengthItems(user) + 1
    }
    ifs.close()
    val resultXvals: Array[Array[Double]] = new Array[Array[Double]](maxUsers)
    val resultXidxs: Array[Array[Int]] = new Array[Array[Int]](maxUsers)
    for (i <- 0 until maxUsers) {
      resultXvals(i) = new Array[Double](lengthItems(i))
      resultXidxs(i) = new Array[Int](lengthItems(i))
      lengthItems(i) = 0
    }
    val ifs2 = Source.fromFile(rawDataFile)
    for (line1 <- ifs2.getLines()) {
      val line = line1.trim()
      val tokens = line.split("\t")
      val i = tokens(0).toInt - 1
      val j = tokens(1).toInt - 1
      val r = tokens(2).toDouble
      resultXvals(i)(lengthItems(i)) = r
      resultXidxs(i)(lengthItems(i)) = j
      if ((i >= 0) && (i < maxUsers)) lengthItems(i) = lengthItems(i) + 1
    }
    val a = (resultXidxs zip resultXvals).map(v => v._1 zip v._2)
    val b = a.map(v => v.sortWith(_._1 < _._1))
    val c = b.map(v => v.unzip).map(v => (v._1.toArray, v._2.toArray)).unzip
    val iss: Array[Array[Int]] = c._1.toArray
    val vss: Array[Array[Double]] = c._2.toArray
    (maxItems, iss, vss)
  }

  object ml {

    val nUsers = 943
    val nItems = 1682
    val mlDataDir = new File("src/main/resources/TestData/ML")

    object u1 {

      lazy val base = new File(mlDataDir, "u1.base")
      lazy val test = new File(mlDataDir, "u1.test")
    }

    object ub {

      lazy val base = new File(mlDataDir, "ub.base")
      lazy val test = new File(mlDataDir, "ub.test")
    }

  }

}

