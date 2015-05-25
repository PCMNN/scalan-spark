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
        val rddIndexes = RDDCollection(sc.makeRDD(rows.map { vec => vec.nonZeroIndices.arr}.seq)) //asRep[RDDCollection[Array[Int]]]
        val rddValues = RDDCollection(sc.makeRDD(rows.map { vec => vec.nonZeroValues.arr}.seq)) //asRep[RDDCollection[Array[Double]]]
        SparkSparseMatrix(rddIndexes, rddValues, numColumns)
      }
      def FactorsMatrix(rows: Coll[AbstractVector[Double]], numColumns: IntRep): Matrix[Double] = {
        val rddValues = RDDCollection(sc.makeRDD(rows.map { vec => vec.items.arr}.seq)) //asRep[RDDCollection[Array[Double]]]
        SparkDenseMatrix(rddValues, numColumns)
      }
      def RandomMatrix(numRows: IntRep, numColumns: IntRep, mean: DoubleRep, stddev: DoubleRep): Matrix[Double] = {
        //val vals = SArray.replicate(numRows, SArray.replicate(numColumns, 0.0))
        val rddVals = sc.makeRDD(SSeq(SArray.replicate(numRows, 0))).map { i:Rep[Int] => SArray.replicate(numColumns, 0.0) }
        SparkDenseMatrix(RDDCollection(rddVals), numColumns)
      }

      override def errorMatrix(mR: Dataset1, mN: Dataset1, mu: DoubleRep)(model: ModelSVD): Dataset1 = {
        //println("[SVD++] mu: " + mu)
        val Tuple(vBu, vBi, mP, mQ, mY) = model
        val nItems = mR.numColumns
        val vsE = (vBu.items zip (mR.rows zip (mP.rows zip mN.rows))).map { case Tuple(bu, vR, vP, vN) =>
          val indices = vN.nonZeroIndices
          val k = IF (indices.length > zeroInt) THEN { one / Math.sqrt(indices.length.toDouble) } ELSE zero
          val vY = mY(indices).reduceByColumns *^ k
          val vBii = vBi.items(vR.nonZeroIndices)
          val vPvY = vP +^ vY
          val mQReduced = mQ.rows(vR.nonZeroIndices) map { row => row dot vPvY }
          //println("[SVD++] vBii: " + vBii.arr.toList)
          //println("mQReduced: " + mQReduced.arr.toList)
          val newValues = (vR.nonZeroValues zip (vBii zip mQReduced)).map { case Tuple(r, bi, pq) =>
            r - mu - bu - bi - pq
          }
          //println("[SVD++] newValues: " + newValues.arr.toList)
          RatingsVector(vR.nonZeroIndices, newValues, nItems)
        }
        RatingsMatrix(vsE, nItems)
      }

      override def calculateMP(mE: Matrix[Double], gamma2: DoubleRep, lambda7: DoubleRep)(svd: ModSVD): Matrix[Double] = {
        val Pair(mP0, mQ0) = svd
        val width = mP0.numColumns
        val vsP = Collection((mE.rows zip mP0.rows).arr).map { case Pair(vE, vP0) =>
          val indices = vE.nonZeroIndices
          val len = indices.length
          val errorsNA = vE.nonZeroValues.map(e => replicate(width, e))
          val mQ0_cut = mQ0(indices)
          val vP1 = FactorsMatrix((mQ0_cut.rows zip errorsNA).map { case Pair(vQ0, ePA) =>
            vQ0 *^ ePA }, mQ0.numColumns).reduceByColumns
          val k = IF (len > zeroInt) THEN power(one - gamma2 * lambda7, len) ELSE one
          (vP1 *^ gamma2) +^ (vP0 *^ k)
        }
        FactorsMatrix(vsP, width)
      }

      // move to Models DSL
      override def calculateMQ(mE: Matrix[Double], mEt: Matrix[Double], mN: Matrix[Double],
                      gamma2: DoubleRep, lambda7: DoubleRep)(svdpp: ModSVDpp): Matrix[Double] = {
        val Tuple(mP0, mQ0, mY0) = svdpp
        val width = mQ0.numColumns
        val vsQ0 = mQ0.rows
        val vsN = mN.rows
        val vsYu = Collection(vsN.arr).map { vN =>
          val indices = vN.nonZeroIndices
          val len = indices.length
          val res = mY0(indices).reduceByColumns
          val mult = IF (len > zeroInt) (one / Math.sqrt(len.toDouble)) ELSE zero
          res *^ mult
          //IF (len > zeroInt) THEN { res /^ Math.sqrt(len.toDouble) } ELSE zeroVector(width)
        }
        val mYu = FactorsMatrix(vsYu, width)
        val vsQ = Collection((mEt.rows/*.columns*/ zip vsQ0).arr).map { case Pair(vEt, vQ0) =>
          val indices = vEt.nonZeroIndices
          IF (indices.length > zeroInt) THEN {
            val errorsNA = vEt.nonZeroValues.map(e => ReplicatedVector(width, e))
            val mErr = FactorsMatrix(errorsNA, width)
            val mP0_cut = mP0(indices)
            val mYu_cut = mYu(indices)
            val vQ1 = ((mP0_cut +^^ mYu_cut) *^^ mErr).reduceByColumns
            vQ0 *^ (one - indices.length.toDouble * gamma2 * lambda7) +^ (vQ1 *^ gamma2)
          } ELSE { vQ0 }
        }
        FactorsMatrix(vsQ, width)
      }

      // move to Models DSL
      override def calculateMY(mE: Matrix[Double], mEt: Matrix[Double], mN: Matrix[Double],
                      gamma2: DoubleRep, lambda7: DoubleRep)(svdpp: ModSVDpp): Matrix[Double] = {
        val Tuple(mP0, mQ0, mY0) = svdpp
        val c0 = one - gamma2 * lambda7
        val width = mP0.numColumns
        val vsY0 = mY0.rows
        val vsNt = mN.transpose.rows
        val vsE = mE.rows
        val coeffs = mE.rows.map(v => power(c0, v.nonZeroValues.length))
        val addDevs = Collection((vsE zip mN.rows.map(v => v.nonZeroIndices.length)).arr).map { case Pair(vE, len) =>
          IF (len > zeroInt) THEN {
            val indices = vE.nonZeroIndices
            val errors  = vE.nonZeroValues
            val errorNA = errors.map(e => replicate(width, e)) // TODO: check if it's OK to discard errors
            // from the formula of updating mY
            val mQ0_cut  = mQ0(indices)
            val c = one / Math.sqrt(len.toDouble)
            FactorsMatrix((mQ0_cut.rows zip errors).map { case Pair(vQc, e) => vQc *^ e }, width).reduceByColumns *^ c
          } ELSE { zeroVector(width) }
        }
        val mAddDev = FactorsMatrix(addDevs, width)
        val vsY = Collection((vsNt zip vsY0).arr).map { case Pair(vNt, vY0) =>
          val indices = vNt.nonZeroIndices
          val len = indices.length
          IF (len > zeroInt) THEN {
            val k = IF (len > zeroInt) THEN power(c0, len) ELSE one
            val coeffY = coeffs(indices).reduce(Multiply) // TODO MATH: check its usage
            val addDevY = mAddDev(indices).reduceByColumns.items
            (vY0 *^ k) +^ addDevY
          } ELSE { vY0 }
        }
        val mY = FactorsMatrix(vsY, width)
        mY
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

    def factories_SVDpp_Train = fun { in: Rep[(ParametersPaired, (RDD[Array[Int]], (RDD[Array[Double]],
      (Int, Double))))] =>
      val Tuple(parametersPaired, idxs, vals, nItems, stddev) = in

      val rddIndexes = SRDDImpl(idxs)
      val rddValues = SRDDImpl(vals)
      val mR: Dataset1 = SparkSparseMatrix(RDDCollection(rddIndexes), RDDCollection(rddValues), nItems)

      val closure = Tuple(parametersPaired, mR, mR)
      val instance = new SparkSVDpp(rddIndexes.context)
      val stateFinal = instance.train(closure, stddev)

      val Tuple(res1, res2,res3, res4, res5) = stateFinal
      Pair(res1._1.items.arr, Pair(res1._2.items.arr, Pair(res2, Pair(res3, Pair(res4, res5)))) )
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
