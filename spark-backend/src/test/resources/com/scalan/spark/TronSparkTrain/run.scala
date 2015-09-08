package com.scalan.spark.TronSparkTrain

import java.io._

import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import com.scalan.spark.method.Methods._

import scala.io.Source

/*
object testUtil {

 type Model = (Int, (Double, (RDD[(Long, Double)], (Array[Double], (RDD[(Long, Array[Double])], (RDD[(Long, Array[Double])], RDD[(Long, Array[Double])]))))))
 type ModelRddElem1to4 = (Int, (Double, ((Long, Double), (Array[Double]))))
 type ModelRddElem5 = (Long, Array[Double])
 type ModelRddElem6 = ModelRddElem5
 type ModelRddElem7 = ModelRddElem5

 def writeHdfsFile(sc: SparkContext, filePath: String, s: String): Unit = {
  sc.makeRDD(Array(s), 1).saveAsTextFile(filePath)
 }

 case class Result(model: Model) {
  def saveModelHdfs(fileName: String) = {
   val (c1, (c2, (rdd3, (arr4, (rdd5, (rdd6, rdd7)))))) = model
   val sc = rdd3.sparkContext
   val c1Bcast = sc.broadcast(c1)
   val c2Bcast = sc.broadcast(c2)
   val arr4Bcast = sc.broadcast(arr4)
   val rdd1to4 = rdd3.mapPartitions { blocks => blocks.map(data3 => (c1, (c2, (data3, arr4)))) }
   rdd1to4.saveAsObjectFile(s"${fileName}-part1")
   rdd5.saveAsObjectFile(s"${fileName}-part2")
   rdd6.saveAsObjectFile(s"${fileName}-part3")
   rdd7.saveAsObjectFile(s"${fileName}-part4")
  }
  def saveModel(fileName: String) = saveModelHdfs(fileName)
 }

 def loadModel(sc: SparkContext, fileName: String): Model = {
  val rdd1to4 = sc.objectFile[ModelRddElem1to4](s"${fileName}-part1")
  val rdd5 = sc.objectFile[ModelRddElem5](s"${fileName}-part2")
  val rdd6 = sc.objectFile[ModelRddElem6](s"${fileName}-part3")
  val rdd7 = sc.objectFile[ModelRddElem7](s"${fileName}-part4")
  val rdd3 = rdd1to4.mapPartitions { blocks => blocks.map(_._2._2._1) }
  val (c1, c2, arr4) = rdd1to4.mapPartitions { blocks => blocks.map { d => (d._1, d._2._1, d._2._2._2) } }.collect.head
  val model = (c1, (c2, (rdd3, (arr4, (rdd5, (rdd6, rdd7))))))
  model
 }
}
 */

object run {

  var globalSparkContext: SparkContext = null

  val maxIterationsTRON = 20
  val maxIterationsTRCG = 100
  val lambda = 1.0
  val stepUpdate = 1.0

  /*
  val maxIterationsTRON = 20
  val maxIterationsTRCG = 100
  val lambda = 1.0
  val eps = 0.1
  val stepUpdate = 1.0

  lazy val arrTrainUB = CsvParser.LoadLiblinearData_IntoDesignMatrix(0.0, 1.0, train.file, -1.0, train.numFeatures, 0)
  lazy val arrTestUB = CsvParser.LoadLiblinearData_IntoDesignMatrix(0.0, 1.0, test.file, -1.0, test.numFeatures, 0)
  */
  /*
  test ("TRON") {
    val tronExp = new LRDslExp with ScalanCtxExp with LALmsCompilerScala
    val tronSeq = new LRDslSeq with LR_TestData
    import tronSeq.{ maxIterationsTRON, maxIterationsTRCG, lambda, eps, stepUpdate }

    val (nItems, arrTrainValues, arrTrainIndices, arrTrainResponses) = tronSeq.arrTrainUB
    val (_, arrTestValues, arrTestIndices, arrTestResponses) = tronSeq.arrTestUB
    val jArrTrain = (arrTrainIndices zip arrTrainValues).map { case(is, vs) => is zip vs }
    val jArrTest = (arrTestIndices zip arrTestValues).map { case(is, vs) => is zip vs }
    val paramsPaired = tronSeq.Tuple(maxIterationsTRON, maxIterationsTRCG, lambda, eps, stepUpdate)
    val in = tronSeq.Tuple(paramsPaired, jArrTrain, jArrTest, nItems, arrTrainResponses, arrTestResponses)
    val debugOutput = tronSeq.LMS_TRON_TrainAndTest(in)
    val stagedOutput = getStagedOutput(tronExp)(tronExp.LMS_TRON_TrainAndTest, "funTrainAndTestTRON", in)
    val arrT = arrTestResponses.map(v => if (v > 0.5) 1.0 else 0.0)
    val outSeq = (debugOutput zip arrT).map { case Tuple2(r, t) =>
      if (Math.abs(r - t) < 0.5) 1.0 else 0.0 }.reduce(_ + _) / arrT.length.toDouble
    val outExp = (stagedOutput zip arrT).map { case Tuple2(r, t) =>
      if (Math.abs(r - t) < 0.5) 1.0 else 0.0 }.reduce(_ + _) / arrT.length.toDouble
    outExp should equal (outSeq)
  }
  */

  def main(args: Array[String]): Unit = {

    val timeStart = System.currentTimeMillis()

    if (args.length != 5) {
      System.out.println("Usage: <host:string> <inputPath:string> <split:string> <eps:int> <outputPath:string>")
      System.exit(0)
    }
    val host = args(0)
    val inputPath = args(1)
    val split = args(2)
    val eps = args(3).toDouble
    val outputPath = args(4)
    val b = 1.0    // should not be changed
    val globalSparkConf = new SparkConf()
      .setAppName("HiTronSparkTrain")
      .setMaster(host)
      .set("spark.executor.extraJavaOptions",
        "-XX:+UseG1GC -XX:ParallelGCThreads=12 -XX:ConcGCThreads=5 -XX:+UseCompressedOops")
    globalSparkContext = new SparkContext(globalSparkConf)

    val timeAfterContext = System.currentTimeMillis()

    val input = trainValues(host, inputPath, split, eps, b)
    val (rddIdxsPartitioned, (rddValsPartitioned, (_, (rddVYPartitioned, _)))) = input
    rddIdxsPartitioned.cache
    rddValsPartitioned.cache
    rddVYPartitioned.cache
    val count = rddIdxsPartitioned.count + rddValsPartitioned.count + rddVYPartitioned.count  // to measure IO time

    val timeAfterInput = System.currentTimeMillis()

    val model = execTrain(input)
    println(s"res = ${model.map(x => f"$x%.6f").toList}")

    val timeAfterComputation = System.currentTimeMillis()

    //  testUtil.Result(model).saveModel(outputPath)
    //
    //  val timeEnd = System.currentTimeMillis()

    val times = List(timeStart, timeAfterContext, timeAfterInput, timeAfterComputation) //, timeEnd)
    val timeIntervals = (times.drop(1) zip times.dropRight(1)).map(p => (p._1 - p._2) / 1000.0)

    val timeSparkContext = timeIntervals(0)
    val timeIo = timeIntervals(1) // + timeIntervals(3)
    val timeComputation = timeIntervals(2)
    val timeTotal = timeSparkContext + timeIo + timeComputation

    println(f"SparkContext time = $timeSparkContext%.3f sec")
    println(f"TRON LR io time = $timeIo%.3f sec")
    println(f"TRON LR computation time = $timeComputation%.3f sec")
    println(f"TRON LR total time = $timeTotal%.3f sec")

    stop()
  }

  def stop() = {
    globalSparkContext.stop()
  }

  private def execTrain(tuple: (RDD[(Long, Array[Int])], (RDD[(Long, Array[Double])], (Int,
    (RDD[(Long, Double)], (Int, (Int, (Double, (Double, Double))))))))) = {

    try {
      new TronSparkTrain()(tuple)
    }
  }

  def trainValues(host: String, inputPath: String, split: String, eps: Double, b: Double) = {

    {
      // This is a hack in order to initialize default parallelism correctly
      val tmp = globalSparkContext.makeRDD(Array(1))
      tmp.count
    }

    def loadLibSVMData(sc: SparkContext, path: String, numPartitions: Int, split: String, b: Double): (RDD[(Long, Array[Int], Array[Double], Double)], Int) = {
      val parsed = sc.textFile(path, numPartitions).map(_.trim).filter(!_.isEmpty)
      val parsedWithLineIds = parsed.zipWithIndex.mapPartitions(block => block.map { case (line, id) => (id, line) })
      val isBias = b >= 0
      val data = parsedWithLineIds.map { case (id, line) => {
        val tokens = line.split(" |\t|\n")
        val y = tokens.head.toDouble
        val (indexes, values) = tokens.tail.map { token =>
          val pair = token.split(split)
          val index = pair(0).toInt - 1     // in file starts from 1, in rdd from 0
          val value = pair(1).toDouble
          (index, value)
        }.unzip
        new Tuple4(id, indexes.toArray, values.toArray, y)
      }}
      val maxId = data.mapPartitions { blocks => blocks.map { case (id, indexes, values, y) => indexes.max } }.reduce(math.max(_, _))
      val res = if (isBias) {
        val maxIdBc = sc.broadcast(maxId)
        val dataRes = data.mapPartitions { blocks => blocks.map { case (id, indexes, values, y) =>
          (id, indexes :+ (maxIdBc.value + 1), values :+ b, y)
        }}
        (dataRes, maxId + 2)
      } else {
        (data, maxId + 1)
      }
      res
    }

    val numPartitions = globalSparkContext.defaultParallelism
    val (data, nColumnsMatrix) = loadLibSVMData(globalSparkContext, inputPath, numPartitions, split, b)

    val rddIdxs = data.mapPartitions { blocks => blocks.map { case d => (d._1, d._2) } }
    val rddIdxsPartitioned = (new org.apache.spark.rdd.OrderedRDDFunctions[Long, Array[Int], (Long, Array[Int])](rddIdxs)).repartitionAndSortWithinPartitions(
      defaultPartitioner(globalSparkContext.defaultParallelism)).cache

    val rddVals = data.mapPartitions { blocks => blocks.map { case d => (d._1, d._3) } }
    val rddValsPartitioned = (new org.apache.spark.rdd.OrderedRDDFunctions[Long, Array[Double], (Long, Array[Double])](rddVals)).repartitionAndSortWithinPartitions(
      defaultPartitioner(globalSparkContext.defaultParallelism)).cache

    val rddVY = data.mapPartitions { blocks => blocks.map { case d => (d._1, d._4) } }
    val rddVYPartitioned = (new org.apache.spark.rdd.OrderedRDDFunctions[Long, Double, (Long, Double)](rddVY)).repartitionAndSortWithinPartitions(
      defaultPartitioner(globalSparkContext.defaultParallelism)).cache

    val params = (maxIterationsTRON, (maxIterationsTRCG, (lambda, (eps, stepUpdate))))
    val res = (rddIdxsPartitioned, (rddValsPartitioned, (nColumnsMatrix, (rddVYPartitioned, params))))
    res
  }
}

