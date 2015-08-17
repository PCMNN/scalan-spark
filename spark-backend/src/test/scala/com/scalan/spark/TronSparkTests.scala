package com.scalan.spark

/**
 * Created by klinov on 8/13/15.
 */

import java.io.File

import com.scalan.spark.backend.SparkScalanCompiler
import la.{SparkLADsl, SparkLADslExp}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.scalatest.BeforeAndAfterAll

import scala.language.reflectiveCalls
import scalan.BaseTests
import scalan.common.OverloadHack.Overloaded1
import scalan.it.ItTestsUtil
import scalan.ml.lr.LRDsl

class TronSparkTests extends BaseTests with BeforeAndAfterAll with ItTestsUtil {
  val pref = new File("test-out/scalan/spark/backend/")
  val globalSparkConf = null
  var globalSparkContext: SparkContext = null

  trait TronSpark extends SparkLADsl with LRDsl {

    trait SparkTronFactory extends Factory {
      def replicate[T: Elem](len: IntRep, v: Rep[T]): Coll[T] = Collection.replicate[T](len, v)
      def indexRange(l: Rep[Int]): Coll[Int] = ???
      def ReplicatedVector(len: IntRep, v: DoubleRep): Vector[Double] = DenseVector(replicate(len, v))
      def zeroVector(len: IntRep): Vector[Double] = DenseVector(Collection.replicate(len, 0.0))
      def fromRows(rows: Coll[AbstractVector[Double]], numColumns: IntRep): Matrix[Double] = ???
      def RatingsVector(nonZeroIndices: Coll[Int], nonZeroValues: Coll[Double], length: IntRep): Vector[Double] =
        SparseVector(nonZeroIndices, nonZeroValues, length)
      def RatingsMatrix(rows: Coll[AbstractVector[Double]], numColumns: IntRep): Matrix[Double] = ???
      def FactorsVector[T: Elem](items: Coll[T]): Vector[T] = ???
      def RandomMatrix(numRows: IntRep, numColumns: IntRep, mean: DoubleRep, stddev: DoubleRep): Matrix[Double] = ???
    }

    class SparkTronExt(sc: Rep[SSparkContext]) extends TRON with SparkTronFactory {
      def FactorsMatrix(rows: Coll[AbstractVector[Double]], numColumns: IntRep): Matrix[Double] = {
        // case dense
//        val rddValues = RDDCollection(sc.makeRDD(rows.map { vec => vec.items.arr }.seq)) //asRep[RDDCollection[Array[Double]]]
//        SparkDenseMatrix(rddValues, numColumns)

        /*
      val rddIndexes = SRDDImpl(idxs)
      val rddValues = SRDDImpl(vals)
      val mR = SparkSparseIndexedMatrix(RDDIndexedCollection(rddIndexes), RDDIndexedCollection(rddValues), nItems)
         */

        // case sparse
        // make as notIndexed
        val rddIndices = RDDCollection(sc.makeRDD(rows.map { vec => vec.nonZeroIndices.arr }.seq))
        val rddValues = RDDCollection(sc.makeRDD(rows.map { vec => vec.nonZeroValues.arr }.seq))
        SparkSparseMatrix(rddIndices, rddValues, numColumns)

//        val rddIndices = RDDIndexedCollection(sc.makeRDD[Array[Int]](rows.map { vec => vec.nonZeroIndices.arr }.seq))
////        val rddIndices = RDDIndexedCollection(SRDD.fromArraySC[Array[Int]](sc, rows.map { vec => vec.nonZeroIndices.arr }.arr))
////        val rddValues = RDDIndexedCollection(sc.makeRDD(rows.map { vec => vec.nonZeroValues.arr }.seq))
//        val rddValues = RDDIndexedCollection(SRDD.fromArraySC[Array[Double]](sc, rows.map { vec => vec.nonZeroValues.arr }.arr))
//
//        SparkSparseIndexedMatrix(rddIndices, rddValues, numColumns)

      }
    }

    // Functions
    def tronSparkTrain = fun { in: Rep[(RDD[(Long,Array[Int])], (RDD[(Long,Array[Double])], (Int, (RDD[(Long, Double)],
      (Int, (Int, (Double, (Double, Double))))))))] =>

      val Tuple(idxsMatrixRDD, valsMatrixRDD, nColumnsMatrix, vYRDD, param1, param2, param3, paramTail) = in
      val parameters = Tuple(param1, param2, param3, paramTail)
      val rddCollIdxsMatrix = RDDIndexedCollection(SRDDImpl(idxsMatrixRDD))
      val rddCollValsMatrix = RDDIndexedCollection(SRDDImpl(valsMatrixRDD))
      val inMatrix = SparkSparseIndexedMatrix(rddCollIdxsMatrix, rddCollValsMatrix, nColumnsMatrix)
      val vY = DenseVector(RDDIndexedCollection(SRDDImpl(vYRDD)))
      val data = (inMatrix, vY)

      val instance = new SparkTronExt(SRDDImpl(vYRDD).context)
//      val model = instance.train(data, parameters)
  val Tuple(maxIterationsTRON, maxIterationsTRCG, lambda, epsInput, stepUpdate) = parameters
//  val (inMatrix, vY) = data
  val epsilon = instance.correctEpsilon(epsInput, vY)
  val Tuple(mX, modelPart) = instance.prepareMatrixForTraining(FALSE, inMatrix)
      val vB0 = instance.ReplicatedVector(mX.numColumns, zero)
      val g0 = instance.gradientRegularized(vY, vB0, mX, lambda, stepUpdate)
      val res = g0.items.arr

      // model.items.arr
      res
    }
  }

  def generationConfig(cmpl: SparkScalanCompiler, pack : String = "gen", command: String = null, getOutput: Boolean = false) =
    cmpl.defaultCompilerConfig.copy(scalaVersion = Some("2.10.4"),
      sbt = cmpl.defaultCompilerConfig.sbt.copy(mainPack = Some(s"com.scalan.spark.$pack"),
        resources = Seq("log4j.properties"),
        extraClasses = Seq("com.scalan.spark.method.Methods"),
        toSystemOut = !getOutput,
        commands = if (command == null) cmpl.defaultCompilerConfig.sbt.commands else Seq(command)))

  class TestClass extends SparkScalanCompiler with SparkLADslExp with TronSpark {
    val sparkContext = globalSparkContext
    val sSparkContext = null
    val conf = null
    val repSparkContext = null
  }

  test("TronSparkTrain Code Gen") {
    val testCompiler = new TestClass {}
    val compiled1 = compileSource(testCompiler)(testCompiler.tronSparkTrain, "TronSparkTrain", generationConfig(testCompiler, "TronSparkTrain", "package"))
  }
}
