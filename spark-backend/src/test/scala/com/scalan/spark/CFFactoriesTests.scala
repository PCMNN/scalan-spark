package com.scalan.spark

/**
 * Created by afilippov on 4/29/15.
 */
import java.io.File

import com.scalan.spark.backend.SparkScalanCompiler
import la.{SparkLADslExp, SparkLADsl}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.scalatest.BeforeAndAfterAll

import scala.language.reflectiveCalls
import scalan.it.ItTestsUtil
import scalan.ml.{CF, CFDslExp, ExampleBL}
import scalan.spark.SparkDsl
import scalan.{BaseTests, ScalanDsl}

class CFFactoriesTests extends BaseTests with BeforeAndAfterAll with ItTestsUtil { suite =>
  val pref = new File("test-out/scalan/spark/backend/")
  val globalSparkConf = null //new SparkConf().setAppName("R/W Broadcast").setMaster("local")
  var globalSparkContext: SparkContext = null

  override def beforeAll() = {
    //globalSparkContext = new SparkContext(globalSparkConf)
  }

  override def afterAll() = {
    //globalSparkContext.stop()
  }

  trait CFFactoriesSparkTests extends SparkLADsl with CF {
    val prefix = suite.pref
    val subfolder = "simple"

    class SparkBL() extends BL {

      def replicate[T: Elem](len: IntRep, v: Rep[T]): Coll[T] = ??? //Collection.replicate[T](len, v)
      def ReplicatedVector(len: IntRep, v: DoubleRep): Vector[Double] = ??? //DenseVector(replicate(len, v))
      def zeroVector(len: IntRep): Vector[Double] = DenseVector(Collection.replicate(len, 0.0))
      def fromRows(rows: Coll[AbstractVector[Double]], numColumns: IntRep): Matrix[Double] =
        ??? //CompoundMatrix.fromRows[Double](rows, numColumns)
      def RatingsVector(nonZeroIndices: Coll[Int], nonZeroValues: Coll[Double], length: IntRep): Vector[Double] =
        SparseVector(nonZeroIndices, nonZeroValues, length)
      def RatingsMatrix(rows: Rep[Collection[AbstractVector[Double]]], numColumns: IntRep): Matrix[Double] = {
        val rddIndexes = rows.map { vec => vec.nonZeroIndices.arr}.asRep[RDDCollection[Array[Int]]]
        val rddValues = rows.map { vec => vec.nonZeroValues.arr}.asRep[RDDCollection[Array[Double]]]
        SparkSparseMatrix(rddIndexes, rddValues, numColumns)
      }
      def FactorsMatrix(rows: Coll[AbstractVector[Double]], numColumns: IntRep): Matrix[Double] =
        ??? //CompoundMatrix.fromRows[Double](rows, numColumns)
      def RandomMatrix(numRows: IntRep, numColumns: IntRep, mean: DoubleRep, stddev: DoubleRep): Matrix[Double] = ???
    }

    class SparkSVDpp(sc: Rep[SSparkContext]) extends SVDpp {
      def replicate[T: Elem](len: IntRep, v: Rep[T]): Coll[T] = Collection.replicate[T](len, v)
      def ReplicatedVector(len: IntRep, v: DoubleRep): Vector[Double] = DenseVector(replicate(len, v))
      def zeroVector(len: IntRep): Vector[Double] = DenseVector(Collection.replicate(len, 0.0))
      def fromRows(rows: Coll[AbstractVector[Double]], numColumns: IntRep): Matrix[Double] = ???
      def RatingsVector(nonZeroIndices: Coll[Int], nonZeroValues: Coll[Double], length: IntRep): Vector[Double] = {
        SparseVector(nonZeroIndices, nonZeroValues, length)
      }

      def RatingsMatrix(rows: Rep[Collection[AbstractVector[Double]]], numColumns: IntRep): Matrix[Double] = {
        val rddIndexes = rows.map { vec => vec.nonZeroIndices.arr}.asRep[RDDCollection[Array[Int]]]
        val rddValues = rows.map { vec => vec.nonZeroValues.arr}.asRep[RDDCollection[Array[Double]]]
        SparkSparseMatrix(rddIndexes, rddValues, numColumns)
      }
      def FactorsMatrix(rows: Coll[AbstractVector[Double]], numColumns: IntRep): Matrix[Double] = {
        val rddValues = rows.map { vec => vec.nonZeroValues.arr}.asRep[RDDCollection[Array[Double]]]
        SparkDenseMatrix(rddValues, numColumns)
      }

      def RandomMatrix(numRows: IntRep, numColumns: IntRep, mean: DoubleRep, stddev: DoubleRep): Matrix[Double] = {
        val vals = SArray.replicate(numRows, SArray.replicate(numColumns, 0.0))
        val rddVals = sc.makeRDD(SSeq(vals))
        SparkDenseMatrix(RDDCollection(rddVals), numColumns)
      }
    }

    def factories_BL_TrainAndTest = fun { in: Rep[(ParametersBL, (RDD[Array[Int]], (RDD[Array[Double]],
      (RDD[Array[Int]], (RDD[Array[Double]], Int)))))] =>
      val Tuple(parametersBL, idxs, vals, idxsT, valsT, nItems) = in

      val rddIndexes = SRDDImpl(idxs)
      val rddValues = SRDDImpl(vals)
      val mR: Dataset1 = SparkSparseMatrix(RDDCollection(rddIndexes), RDDCollection(rddValues), nItems)


      val rddIndexesT = SRDDImpl(idxsT)
      val rddValuesT = SRDDImpl(valsT)
      val mT: Dataset1 = SparkSparseMatrix(RDDCollection(rddIndexesT), RDDCollection(rddValuesT), nItems)

      val closure = Pair(parametersBL, mR)
      val instance = new SparkBL()
      val stateFinal = instance.train(closure)
      val rmse = instance.predict(mT, stateFinal._1)
      rmse
    }

    def factories_errorMatrix = fun { in: Rep[(ParametersBL, (RDD[Array[Int]], (RDD[Array[Double]], Int)))] =>
      val Tuple(parametersBL, idxs, vals, nItems) = in

      val rddIndexes = SRDDImpl(idxs)
      val rddValues = SRDDImpl(vals)
      val mR: Dataset1 = SparkSparseMatrix(RDDCollection(rddIndexes), RDDCollection(rddValues), nItems)

      val closure = Pair(parametersBL, mR)
      val instance = new SparkBL()
      val nUsers = mR.numRows
      val vBu0: Vector[Double] = instance.zeroVector(nUsers)
      val vBi0: Vector[Double] = instance.zeroVector(nItems)
      val model0: ModBL = Pair(vBu0, vBi0)
      val mu = instance.average(mR)
      val eM = instance.errorMatrix(mR, mu)(model0)
      eM.rows.arr.map {v => v.nonZeroItems.arr}
    }

    def factories_SVDpp_TrainAndTest = fun { in: Rep[(ParametersPaired, (RDD[Array[Int]], (RDD[Array[Double]],
      (RDD[Array[Int]], (RDD[Array[Double]], (Int, Double))))))] =>
      val Tuple(parametersPaired, idxs, vals, idxsT, valsT, nItems, stddev) = in

      val rddIndexes = SRDDImpl(idxs)
      val rddValues = SRDDImpl(vals)
      val mR: Dataset1 = SparkSparseMatrix(RDDCollection(rddIndexes), RDDCollection(rddValues), nItems)


      val rddIndexesT = SRDDImpl(idxsT)
      val rddValuesT = SRDDImpl(valsT)
      val mT: Dataset1 = SparkSparseMatrix(RDDCollection(rddIndexesT), RDDCollection(rddValuesT), nItems)

      val closure = Tuple(parametersPaired, mR, mR)
      val instance = new SparkSVDpp(rddIndexes.context)
      val stateFinal = instance.train(closure, stddev)
      val rmse = instance.predict(mT, mT, stateFinal._1)
      rmse
    }
  }

  def generationConfig(cmpl: SparkScalanCompiler, pack : String = "gen", command: String = null, getOutput: Boolean = false) =
    cmpl.defaultCompilerConfig.copy(scalaVersion = Some("2.10.4"),
      sbt = cmpl.defaultCompilerConfig.sbt.copy(mainPack = Some(s"com.scalan.spark.$pack"),
        resources = Seq("log4j.properties"),
        extraClasses = Seq("com.scalan.spark.method.Methods"),
        toSystemOut = !getOutput,
        commands = if (command == null) cmpl.defaultCompilerConfig.sbt.commands else Seq(command)))

  /*test("Error matrix Code Gen") {

    val testCompiler = new SparkScalanCompiler with SparkLADslExp with CFFactoriesSparkTests with CFDslExp {
      self =>
      val sparkContext = globalSparkContext
      val sSparkContext = null //ExpSSparkContextImpl(globalSparkContext)
      val conf = null //SSparkConf().setAppName(toRep("MVMTests")).setMaster(toRep("local[8]"))
      val repSparkContext = null //SSparkContext(conf)
    }

    val compiled = compileSource(testCompiler)(testCompiler.factories_errorMatrix, "factories_errorMatrix", generationConfig(testCompiler, "factories_errorMatrix", "package"))
  }
  test("CF_BL Code Gen") {
    val testCompiler = new SparkScalanCompiler with SparkLADslExp with CFFactoriesSparkTests with CFDslExp {
      self =>
      val sparkContext = globalSparkContext
      val sSparkContext = null //ExpSSparkContextImpl(globalSparkContext)
      val conf = null //SSparkConf().setAppName(toRep("MVMTests")).setMaster(toRep("local[8]"))
      val repSparkContext = null //SSparkContext(conf)
    }
    val compiled1 = compileSource(testCompiler)(testCompiler.factories_BL_TrainAndTest, "factories_BL_TrainAndTest", generationConfig(testCompiler, "factories_BL_TrainAndTest", "package"))
  } */
  test("CF_SVDpp Code Gen") {
    val testCompiler = new SparkScalanCompiler with SparkLADslExp with CFFactoriesSparkTests with CFDslExp {
      self =>
      val sparkContext = globalSparkContext
      val sSparkContext = null //ExpSSparkContextImpl(globalSparkContext)
      val conf = null //SSparkConf().setAppName(toRep("MVMTests")).setMaster(toRep("local[8]"))
      val repSparkContext = null //SSparkContext(conf)
    }
    val compiled1 = compileSource(testCompiler)(testCompiler.factories_SVDpp_TrainAndTest, "factories_SVDpp_TrainAndTest", generationConfig(testCompiler, "factories_SVDpp_TrainAndTest", "package"))
  }

}
