/**
 * Created by afilippov on 4/20/15.
 */
package com.scalan.spark


import java.io.File

import com.scalan.spark.backend.SparkScalanCompiler
import la.{SparkLADslExp, SparkLADsl}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.scalatest.BeforeAndAfterAll

import scala.language.reflectiveCalls
import scalan.it.ItTestsUtil
import scalan.spark.SparkDsl
import scalan.{BaseTests, ScalanDsl}

class BackendLATests extends BaseTests with BeforeAndAfterAll with ItTestsUtil { suite =>
  val pref = new File("test-out/scalan/spark/backend/")
  val globalSparkConf = null //new SparkConf().setAppName("R/W Broadcast").setMaster("local")
  var globalSparkContext: SparkContext = null

  override def beforeAll() = {
    //globalSparkContext = new SparkContext(globalSparkConf)
  }

  override def afterAll() = {
    //globalSparkContext.stop()
  }

  trait BackendLASparkTests extends SparkLADsl {
    val prefix = suite.pref
    val subfolder = "simple"

    lazy val sdmvm = fun { in: Rep[(SparkContext, (Array[Array[Int]], (Array[Array[Double]], Array[Double])))] =>
      val Tuple(sc, idxs, vals, vec) = in
      val rddIndexes: Rep[SRDD[Array[Int]]] = SSparkContextImpl(sc).makeRDD(SSeq(idxs), 2)
      val rddValues: Rep[SRDD[Array[Double]]] = SSparkContextImpl(sc).makeRDD(SSeq(vals), 2)
      val numCols = vec.length

      val matrix: Matrix[Double] = SparkSparseMatrix(RDDCollection(rddIndexes), RDDCollection(rddValues), numCols)
      val vector: Vector[Double] = DenseVector(Collection(vec))
      (matrix * vector).items.arr

    }

    lazy val makeNewRDD = fun { in: Rep[RDD[Int]] =>
      val newRDD = SRDD.fromArraySC(SRDDImpl(in).context, array_rangeFrom0(10))
      newRDD.wrappedValueOfBaseType
    }

    lazy val flatMapFun = fun { in: Rep[RDD[Array[Int]]] =>
      val res = SRDDImpl(in).flatMap {arg: Rep[Array[Int]] => SSeq(arg) }
      res.wrappedValueOfBaseType
    }

    lazy val avFun = fun {in: Rep[(RDD[Array[Int]], (RDD[Array[Double]],Int))] =>
      val Tuple(idxs, vals, nItems) = in
      val rddIndexes = SRDDImpl(idxs)
      val rddValues = SRDDImpl(vals)
      val mR = SparkSparseMatrix(RDDCollection(rddIndexes), RDDCollection(rddValues), nItems)

      def avg(matrix: Matrix[Double]) = {
        val coll = matrix.rows.flatMap(v => v.nonZeroValues)
        coll.reduce / coll.length.toDouble
      }

      avg(mR)
    }

    lazy val countNonZerosFun = fun {in: Rep[(RDD[Array[Int]], (RDD[Array[Double]],Int))] =>
      val Tuple(idxs, vals, nItems) = in
      val rddIndexes = SRDDImpl(idxs)
      val rddValues = SRDDImpl(vals)
      val mR = SparkSparseMatrix(RDDCollection(rddIndexes), RDDCollection(rddValues), nItems)
      mR.countNonZeroesByColumns.items.arr
    }

    lazy val reduceByColumnsFun = fun {in: Rep[(RDD[Array[Int]], (RDD[Array[Double]],Int))] =>
      val Tuple(idxs, vals, nItems) = in
      val rddIndexes = SRDDImpl(idxs)
      val rddValues = SRDDImpl(vals)
      val mR = SparkSparseMatrix(RDDCollection(rddIndexes), RDDCollection(rddValues), nItems)
      mR.reduceByColumns.items.arr
    }

    lazy val transposeFun = fun {in: Rep[(RDD[Array[Int]], (RDD[Array[Double]],Int))] =>
      val Tuple(idxs, vals, nItems) = in
      val rddIndexes = SRDDImpl(idxs)
      val rddValues = SRDDImpl(vals)
      val mR = SparkSparseMatrix(RDDCollection(rddIndexes), RDDCollection(rddValues), nItems)
      val transp = mR.transpose.asRep[SparkSparseMatrix[Double]]
      transp.rows.arr.map({vec => vec.nonZeroItems.arr})
    }
  }

  def generationConfig(cmpl: SparkScalanCompiler, pack : String = "gen", command: String = null, getOutput: Boolean = false) =
    cmpl.defaultCompilerConfig.copy(scalaVersion = Some("2.10.4"),
      sbt = cmpl.defaultCompilerConfig.sbt.copy(mainPack = Some(s"com.scalan.spark.$pack"),
        resources = Seq("log4j.properties"),
        extraClasses = Seq("com.scalan.spark.method.Methods"),
        toSystemOut = !getOutput,
        commands = if (command == null) cmpl.defaultCompilerConfig.sbt.commands else Seq(command)))

  test("MakeNewRDD Code Gen") {

    val testCompiler = new SparkScalanCompiler with SparkLADslExp with BackendLASparkTests {
      self =>
      val sparkContext = globalSparkContext
      val sSparkContext = null //ExpSSparkContextImpl(globalSparkContext)
      val conf = null //SSparkConf().setAppName(toRep("MVMTests")).setMaster(toRep("local[8]"))
      val repSparkContext = null //SSparkContext(conf)
    }

    val compiled = compileSource(testCompiler)(testCompiler.makeNewRDD, "makeNewRDD", generationConfig(testCompiler, "makeNewRDD", "package"))
  }
  test("FlatMap Code Gen") {

    val testCompiler = new SparkScalanCompiler with SparkLADslExp with BackendLASparkTests {
      self =>
      val sparkContext = globalSparkContext
      val sSparkContext = null //ExpSSparkContextImpl(globalSparkContext)
      val conf = null //SSparkConf().setAppName(toRep("MVMTests")).setMaster(toRep("local[8]"))
      val repSparkContext = null //SSparkContext(conf)
    }

    val compiled = compileSource(testCompiler)(testCompiler.flatMapFun, "flatMapFun", generationConfig(testCompiler, "flatMapFun", "package"))
  }
  test("Average Code Gen") {

    val testCompiler = new SparkScalanCompiler with SparkLADslExp with BackendLASparkTests {
      self =>
      val sparkContext = globalSparkContext
      val sSparkContext = null //ExpSSparkContextImpl(globalSparkContext)
      val conf = null //SSparkConf().setAppName(toRep("MVMTests")).setMaster(toRep("local[8]"))
      val repSparkContext = null //SSparkContext(conf)
    }

    val compiled = compileSource(testCompiler)(testCompiler.avFun, "avFun", generationConfig(testCompiler, "avFun", "package"))

  }
  test("countNonZeros Code Gen") {

    val testCompiler = new SparkScalanCompiler with SparkLADslExp with BackendLASparkTests {
      self =>
      val sparkContext = globalSparkContext
      val sSparkContext = null //ExpSSparkContextImpl(globalSparkContext)
      val conf = null //SSparkConf().setAppName(toRep("MVMTests")).setMaster(toRep("local[8]"))
      val repSparkContext = null //SSparkContext(conf)
    }
    val compiled = compileSource(testCompiler)(testCompiler.countNonZerosFun, "countNonZeros", generationConfig(testCompiler, "countNonZeros", "package"))
  }
  test("ReduceByColumns Code Gen") {

    val testCompiler = new SparkScalanCompiler with SparkLADslExp with BackendLASparkTests {
      self =>
      val sparkContext = globalSparkContext
      val sSparkContext = null //ExpSSparkContextImpl(globalSparkContext)
      val conf = null //SSparkConf().setAppName(toRep("MVMTests")).setMaster(toRep("local[8]"))
      val repSparkContext = null //SSparkContext(conf)
    }
    val compiled = compileSource(testCompiler)(testCompiler.reduceByColumnsFun, "reduceByColumns", generationConfig(testCompiler, "reduceByColumns", "package"))
  }
  test("Transpose Code Gen") {

    val testCompiler = new SparkScalanCompiler with SparkLADslExp with BackendLASparkTests {
      self =>
      val sparkContext = globalSparkContext
      val sSparkContext = null //ExpSSparkContextImpl(globalSparkContext)
      val conf = null //SSparkConf().setAppName(toRep("MVMTests")).setMaster(toRep("local[8]"))
      val repSparkContext = null //SSparkContext(conf)
    }
    val compiled = compileSource(testCompiler)(testCompiler.transposeFun, "transposeFun", generationConfig(testCompiler, "transposeFun", "package"))
  }
  test("SDMVM Code Gen") {

    val testCompiler = new SparkScalanCompiler with SparkLADslExp with BackendLASparkTests {
      self =>
      val sparkContext = globalSparkContext
      val sSparkContext = null //ExpSSparkContextImpl(globalSparkContext)
      val conf = null //SSparkConf().setAppName(toRep("MVMTests")).setMaster(toRep("local[8]"))
      val repSparkContext = null //SSparkContext(conf)
    }
    val compiled = compileSource(testCompiler)(testCompiler.sdmvm, "sdmvm", generationConfig(testCompiler, "sdmvm", "package"))
  }

}


