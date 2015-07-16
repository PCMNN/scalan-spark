package com.scalan.spark.SVDppSpark_Train

import java.io.File

import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import com.scalan.spark.method.Methods._

import scala.io.Source

object run {
  val globalSparkConf = new SparkConf().setAppName("SVDppSpark_Train").setMaster("local[8]").
    /*val globalSparkConf = new SparkConf().setAppName("SVDppSpark_Train").
       setMaster("spark://10.122.85.159:7178").
       setJars(Array("SVDppSpark_Train.jar")). */
    set("spark.executor.extraJavaOptions",
      "-XX:+UseG1GC -XX:ParallelGCThreads=12 -XX:ConcGCThreads=5 -XX:+UseCompressedOops").
    set("spark.executor.memory", "2g").set("spark.cores.max", "48")

  var globalSparkContext: SparkContext = null

  type ParametersPaired = (Int, (Double, (Double, (Double, (Double, (Double, (Int, Double)))))))
  lazy val maxIterations = 20
  lazy val delta = 1e-6
  lazy val gamma1 = 0.00005
  lazy val gamma2 = 0.00001
  lazy val lambda6 = 0.005
  lazy val lambda7 = 0.15
  lazy val stepDecrease = 0.9
  lazy val width = 10
  lazy val stddev = 0.0027
  val (_, arrTrainIndices, arrTrainValues) = LoadMovieLensData_IntoRatingMatrix(0.0, 1.0, nf.u1.base, -1.0, nf.nUsers, nf.nItems)


  def main(args: Array[String]): Unit = {
    println("Start")
    val v = exec(defaultValues())
    val (mu, (vBu, (vBi, (mP, (mQ, (mY, (iters, (err, (flag, (stepDecrease)))))))))) = v
    println("sqrtErr: " + err)
  }

  def execute(): Unit = {
    exec(values())
  }

  def stop() = {
    globalSparkContext.stop()
  }

  private def exec(tuple: (ParametersPaired, (RDD[(Long, Array[Int])], (RDD[(Long, Array[Double])], (Int, Double))))) = {
    try {
      new SVDppSpark_Train()(tuple)
    }
    finally stop()
  }

  def defaultValues() = values()

  def values() = {
    globalSparkContext = new SparkContext(globalSparkConf)

    {
      // This is a hack in order to initialize default parallelism correctly
      val tmp = globalSparkContext.makeRDD(Array(1))
      tmp.count
    }
    val params = (maxIterations, (delta, (gamma1, (gamma2, (lambda6, (lambda7, (width, stepDecrease)))))))

    val rddIdxs = globalSparkContext.makeRDD(arrTrainIndices.indices.map { _.toLong} zip arrTrainIndices, globalSparkContext.defaultParallelism)
    val rddIdxsPartitioned = (new org.apache.spark.rdd.OrderedRDDFunctions[Long, Array[Int], (Long, Array[Int])](rddIdxs)).repartitionAndSortWithinPartitions(
      defaultPartitioner(globalSparkContext.defaultParallelism)).cache

    val rddVals = globalSparkContext.makeRDD(arrTrainValues.indices.map {_.toLong} zip arrTrainValues, globalSparkContext.defaultParallelism)
    val rddValsPartitioned = (new org.apache.spark.rdd.OrderedRDDFunctions[Long, Array[Double], (Long, Array[Double])](rddVals)).repartitionAndSortWithinPartitions(
      defaultPartitioner(globalSparkContext.defaultParallelism)).cache

    val res = (params, (rddIdxsPartitioned, (rddValsPartitioned, (nf.nItems, stddev))))
    rddIdxsPartitioned.unpersist()
    rddValsPartitioned.unpersist()
    res
  }

  def LoadMovieLensData_IntoRatingMatrix(start: Double, end: Double, rawDataFile: File, outputMinValue: Double, maxUsers: Int, maxItems: Int):
  (Int, Array[Array[Int]], Array[Array[Double]]) = {
    val ifs = Source.fromFile(rawDataFile)
    val lengthItems: Array[Int] = new Array[Int](maxUsers)
    for (line1 <- ifs.getLines()) {
      val line = line1.trim()
      val tokens = line.split("::")
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
      val tokens = line.split("::")
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

  object nf {

    val nUsers = 943 // 95313
    val nItems = 1682 // 3651
    val mlDataDir = new File("src/main/resources/TestData/ML")

    object u1 {

      lazy val base = new File(mlDataDir, "ub.base") // new File(mlDataDir, "nf.base")
      lazy val test = new File(mlDataDir, "ub.base") // new File(mlDataDir, "nf.base")
    }
  }
}

