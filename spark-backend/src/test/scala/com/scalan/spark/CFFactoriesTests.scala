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
import scalan.ml.cf.{CFDslExp, CFDsl}
import scalan.ml.{MLDsl, ML_Factories}
import scalan.spark.SparkDsl
import scalan.{BaseTests, ScalanDsl}

class CFFactoriesTests extends BaseTests with BeforeAndAfterAll with ItTestsUtil { suite =>
  val pref = new File("test-out/scalan/spark/backend/")
  val globalSparkConf = null
  var globalSparkContext: SparkContext = null


  trait CFFactoriesSparkTests extends SparkLADsl with CFDsl {
    val prefix = suite.pref
    val subfolder = "simple"

    trait SparkBLFactory extends Factory {

      def replicate[T: Elem](len: IntRep, v: Rep[T]): Coll[T] = ??? //Collection.replicate[T](len, v)
      def indexRange(l: Rep[Int]): Coll[Int] = ???
      def ReplicatedVector(len: IntRep, v: DoubleRep): Vector[Double] = ??? //DenseVector(replicate(len, v))
      def zeroVector(len: IntRep): Vector[Double] = DenseVector(Collection.replicate(len, 0.0))
      def fromRows(rows: Coll[AbstractVector[Double]], numColumns: IntRep): Matrix[Double] =
        ??? //CompoundMatrix.fromRows[Double](rows, numColumns)
      def RatingsVector(nonZeroIndices: Coll[Int], nonZeroValues: Coll[Double], length: IntRep): Vector[Double] =
        SparseVector(nonZeroIndices, nonZeroValues, length)
      def FactorsMatrix(rows: Coll[AbstractVector[Double]], numColumns: IntRep): Matrix[Double] =
        ??? //CompoundMatrix.fromRows[Double](rows, numColumns)
      def RandomMatrix(numRows: IntRep, numColumns: IntRep, mean: DoubleRep, stddev: DoubleRep): Matrix[Double] = ???
    }

    class SparkBLExt(sc: Rep[SSparkContext]) extends BL with SparkBLFactory {
      def RatingsMatrix(rows: Rep[Collection[AbstractVector[Double]]], numColumns: IntRep): Matrix[Double] = {
        val rddIndexes: Rep[RDDCollection[Array[Int]]] = RDDCollection( sc.makeRDD(rows.map { vec => vec.nonZeroIndices.arr}.seq) )
        val rddValues:  Rep[RDDCollection[Array[Double]]] = RDDCollection( sc.makeRDD( rows.map { vec => vec.nonZeroValues.arr}.seq))
        SparkSparseMatrix(rddIndexes, rddValues, numColumns)
      }
    }

    trait SparkSVDppFactory extends Factory {
      def replicate[T: Elem](len: IntRep, v: Rep[T]): Coll[T] = Collection.replicate[T](len, v)
      def indexRange(l: Rep[Int]): Coll[Int] = ???
      def ReplicatedVector(len: IntRep, v: DoubleRep): Vector[Double] = DenseVector(replicate(len, v))
      def zeroVector(len: IntRep): Vector[Double] = DenseVector(Collection.replicate(len, 0.0))
      def fromRows(rows: Coll[AbstractVector[Double]], numColumns: IntRep): Matrix[Double] = ???
      def RatingsVector(nonZeroIndices: Coll[Int], nonZeroValues: Coll[Double], length: IntRep): Vector[Double] = {
        SparseVector(nonZeroIndices, nonZeroValues, length)
      }
    }

    class SparkSVDppExt(sc: Rep[SSparkContext]) extends SVDpp with SparkSVDppFactory {
      def RatingsMatrix(rows: Rep[Collection[AbstractVector[Double]]], numColumns: IntRep): Matrix[Double] = {
        val rddIndexes = RDDCollection(sc.makeRDD(rows.map { vec => vec.nonZeroIndices.arr}.seq)) //asRep[RDDCollection[Array[Int]]]
        val rddValues = RDDCollection(sc.makeRDD(rows.map { vec => vec.nonZeroValues.arr}.seq)) //asRep[RDDCollection[Array[Double]]]
        SparkSparseMatrix(rddIndexes, rddValues, numColumns)
      }
      def FactorsMatrix(rows: Coll[AbstractVector[Double]], numColumns: IntRep): Matrix[Double] = {
        val rddValues = RDDCollection(sc.makeRDD(rows.map { vec => vec.items.arr}.seq)) //asRep[RDDCollection[Array[Double]]]
        SparkDenseMatrix(rddValues, numColumns)
      }
      def RandomMatrix(numRows: IntRep, numColumns: IntRep, mean: DoubleRep, stddev: DoubleRep): Matrix[Double] = {
        val rddVals = sc.makeRDD(SSeq(SArray.replicate(numRows, 0))).map { i: Rep[Int] => SArray.replicate(numColumns, 0.0)}
        SparkDenseMatrix(RDDCollection(rddVals), numColumns)
      }
    }

    def factories_BL_TrainAndTest = fun { in: Rep[(ParametersTupleBL, (RDD[Array[Int]], (RDD[Array[Double]],
      (RDD[Array[Int]], (RDD[Array[Double]], Int)))))] =>
      val Tuple(parametersTupleBL, idxs, vals, idxsT, valsT, nItems) = in
      val Tuple(maxIterations, convergeLimit, gamma1, lambda6, coeffDecrease) = parametersTupleBL

      val rddIndexes = SRDDImpl(idxs)
      val rddValues = SRDDImpl(vals)
      val mR  = SparkSparseMatrix(RDDCollection(rddIndexes), RDDCollection(rddValues), nItems)

      val rddIndexesT = SRDDImpl(idxsT)
      val rddValuesT = SRDDImpl(valsT)
      val mT = SparkSparseMatrix(RDDCollection(rddIndexesT), RDDCollection(rddValuesT), nItems)

      val parameters = new ParametersBL(mR.numRows, nItems, maxIterations, convergeLimit, gamma1, lambda6, coeffDecrease)
      val instance = new SparkBLExt(rddIndexes.context)
      val stateFinal = instance.train(parameters, mR)
      val rmse = instance.predict(mT, stateFinal._1)
      rmse
    }

    def factories_errorMatrix = fun { in: Rep[(ParametersTupleBL, (RDD[Array[Int]], (RDD[Array[Double]], Int)))] =>
      val Tuple(parametersTupleBL, idxs, vals, nItems) = in
      val Tuple(maxIterations, convergeLimit, gamma1, lambda6, coeffDecrease) = parametersTupleBL

      val rddIndexes = SRDDImpl(idxs)
      val rddValues = SRDDImpl(vals)
      val mR = SparkSparseMatrix(RDDCollection(rddIndexes), RDDCollection(rddValues), nItems)

      val parameters = new ParametersBL(mR.numRows, nItems, maxIterations, convergeLimit, gamma1, lambda6, coeffDecrease)
      val instance = new SparkBLExt(rddIndexes.context)
      val model = instance.createModel(parameters)
      val mu = instance.average(mR)
      val eM = instance.errorMatrix(mR, mu)(model)
      eM.rows.arr.map {v => v.nonZeroItems.arr}
    }

    def factories_SVDpp_TrainAndTest = fun { in: Rep[(ParametersTupleSVD, (RDD[Array[Int]], (RDD[Array[Double]],
      (RDD[Array[Int]], (RDD[Array[Double]], (Int, Double))))))] =>
      val Tuple(parametersTupleSVD, idxs, vals, idxsT, valsT, nItems, stddev) = in
      val Tuple(maxIterations, convergeLimit, gamma1, gamma2, lambda6, lambda7, width, coeffDecrease) = parametersTupleSVD
      val rddIndexes = SRDDImpl(idxs)
      val rddValues = SRDDImpl(vals)
      val mR = SparkSparseMatrix(RDDCollection(rddIndexes), RDDCollection(rddValues), nItems)


      val rddIndexesT = SRDDImpl(idxsT)
      val rddValuesT = SRDDImpl(valsT)
      val mT = SparkSparseMatrix(RDDCollection(rddIndexesT), RDDCollection(rddValuesT), nItems)

      val parameters = new ParametersSVD(mR.numRows, nItems, maxIterations, convergeLimit, gamma1,
        gamma2, lambda6, lambda7, width, coeffDecrease, stddev)
      val svdpp = new SparkSVDppExt(rddIndexes.context)
      val stateFinal = svdpp.train(parameters, (mR, mR))
      val rmse = svdpp.predict((mT, mT), stateFinal._1)

      rmse
    }

    def factories_SVDpp_Train = fun { in: Rep[(ParametersTupleSVD, (RDD[Array[Int]], (RDD[Array[Double]], (Int, Double))))] =>
      val Tuple(parametersTupleSVD, idxs, vals, nItems, stddev) = in
      val Tuple(maxIterations, convergeLimit, gamma1, gamma2, lambda6, lambda7, width, coeffDecrease) = parametersTupleSVD

      val rddIndexes = SRDDImpl(idxs)
      val rddValues = SRDDImpl(vals)
      val mR = SparkSparseMatrix(RDDCollection(rddIndexes), RDDCollection(rddValues), nItems)

      val parameters = new ParametersSVD(mR.numRows, nItems, maxIterations, convergeLimit, gamma1,
        gamma2, lambda6, lambda7, width, coeffDecrease, stddev)
      val svdpp = new SparkSVDppExt(rddIndexes.context)
      val stateFinal = svdpp.train(parameters, (mR, mR))
      val Tuple(model, iters, rmse) = stateFinal
      val Tuple(res1, res2, res3, res4, res5) = model

      // For simplicity, we will not return full model here. Just 2 vectors
      Pair(res1.items.arr, Pair(res2.items.arr, Pair(iters, rmse)))
    }
  }

  def generationConfig(cmpl: SparkScalanCompiler, pack : String = "gen", command: String = null, getOutput: Boolean = false) =
    cmpl.defaultCompilerConfig.copy(scalaVersion = Some("2.10.4"),
      sbt = cmpl.defaultCompilerConfig.sbt.copy(mainPack = Some(s"com.scalan.spark.$pack"),
        resources = Seq("log4j.properties"),
        extraClasses = Seq("com.scalan.spark.method.Methods"),
        toSystemOut = !getOutput,
        commands = if (command == null) cmpl.defaultCompilerConfig.sbt.commands else Seq(command)))

  test("Error matrix Code Gen") {

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
  }

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

  test("CF_SVDpp Train Code Gen") {
    val testCompiler = new SparkScalanCompiler with SparkLADslExp with CFFactoriesSparkTests with CFDslExp {
      self =>
      val sparkContext = globalSparkContext
      val sSparkContext = null
      val conf = null
      val repSparkContext = null
    }
    val compiled1 = compileSource(testCompiler)(testCompiler.factories_SVDpp_Train, "factories_SVDpp_Train", generationConfig(testCompiler, "factories_SVDpp_Train", "package"))
  }

}

