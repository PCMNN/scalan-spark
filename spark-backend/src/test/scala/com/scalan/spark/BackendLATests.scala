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
import scalan.ml.{CFDslExp, ExampleBL}
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

  trait BackendLASparkTests extends SparkLADsl with ExampleBL {
    val prefix = suite.pref
    val subfolder = "simple"
    //lazy val sparkContextElem = element[SparkContext]
    //lazy val defaultSparkContextRep = sparkContextElem.defaultRepValue
    //lazy val sparkConfElem = element[SparkConf]
    //lazy val defaultSparkConfRep = sparkConfElem.defaultRepValue

    lazy val sdmvm = fun { in: Rep[(Array[Array[Int]], (Array[Array[Double]], Array[Double]))] =>
      val Tuple(idxs, vals, vec) = in
      val rddIndexes: Rep[SRDD[Array[Int]]] = repSparkContext.makeRDD(SSeq(idxs), 2)
      val rddValues: Rep[SRDD[Array[Double]]] = repSparkContext.makeRDD(SSeq(vals), 2)
      val numCols = vec.length

      val matrix: Matrix[Double] = SparkSparseMatrix(RDDCollection(rddIndexes), RDDCollection(rddValues), numCols)
      val vector: Vector[Double] = DenseVector(Collection(vec))
      (matrix * vector).items.arr

    }

    override def errorMatrix(mR1: Dataset1, mu: DoubleRep)(model: ModBL): Dataset1 = {
      val Pair(vBu, vBi) = model
      val mR = mR1.asRep[SparkSparseMatrix[Double]]
      val nItems = mR.numColumns
      val vsE = (mR.rddIdxs zip mR.rddVals zip vBu.items).map {
        case Pair(Pair(idxs, vals), bu) =>
          val vBii = vBi.items.arr(idxs)
          val newValues = (vals zip vBii).map { case Pair(r, bi) =>  r - mu - bu - bi }
          newValues
      }
      val newIdxs = mR.rddIdxs
      SparkSparseMatrix(newIdxs, RDDCollection(SRDD.fromArraySC(mR.sc, vsE.arr)), nItems)
    }

    lazy val trainAndTestBL = fun { in: Rep[(ParametersBL, (RDD[Array[Int]], (RDD[Array[Double]],
      (RDD[Array[Int]], (RDD[Array[Double]], Int)))))] =>
      val Tuple(parametersBL, idxs, vals, idxsT, valsT, nItems) = in

      val rddIndexes = SRDDImpl(idxs)
      val rddValues = SRDDImpl(vals)
      val mR: Dataset1 = SparkSparseMatrix(RDDCollection(rddIndexes), RDDCollection(rddValues), nItems)


      val rddIndexesT = SRDDImpl(idxsT)
      val rddValuesT = SRDDImpl(valsT)
      val mT: Dataset1 = SparkSparseMatrix(RDDCollection(rddIndexesT), RDDCollection(rddValuesT), nItems)


      val closure1 = Pair(parametersBL, mR)
      val nUsers = mR.numRows

      val vBu0: Vector[Double] = DenseVector(Collection.replicate(nUsers, 0.0)) //DenseVector(RDDCollection(SRDDImpl(zeroArr)))
      val vBi0: Vector[Double] = DenseVector(Collection.replicate(nItems, 0.0)) //DenseVector(RDDCollection(SRDDImpl(zeroArr)))
      val mdl0: ModBL = Pair(vBu0, vBi0)
      val stateFinal = train(closure1, mdl0)

      //val stateFinal = train(closure1)
      val rmse = predict(mT, stateFinal._1)
      rmse
    }

    lazy val trainBL = fun { in: Rep[(ParametersBL, (RDD[Array[Int]], (RDD[Array[Double]], Int)))] =>
      val Tuple(parametersBL, idxs, vals, nItems) = in

      val rddIndexes = SRDDImpl(idxs)
      val rddValues = SRDDImpl(vals)
      val mR: Dataset1 = SparkSparseMatrix(RDDCollection(rddIndexes), RDDCollection(rddValues), nItems)

      val closure1 = Pair(parametersBL, mR)
      val nUsers = mR.numRows

      val vBu0: Vector[Double] = DenseVector(Collection.replicate(nUsers, 0.0)) //DenseVector(RDDCollection(SRDDImpl(zeroArr)))
      val vBi0: Vector[Double] = DenseVector(Collection.replicate(nItems, 0.0)) //DenseVector(RDDCollection(SRDDImpl(zeroArr)))
      val mdl0: ModBL = Pair(vBu0, vBi0)
      val stateFinal = train(closure1, mdl0)

      val Tuple(res1, res2, res3, res4, res5) = stateFinal
      Pair(res1._1.items.arr, Pair(res1._2.items.arr, Pair(res2, Pair(res3, Pair(res4, res5)))) )
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
      val mR: Dataset1 = SparkSparseMatrix(RDDCollection(rddIndexes), RDDCollection(rddValues), nItems)
      average(mR)
    }

    lazy val countNonZerosFun = fun {in: Rep[(RDD[Array[Int]], (RDD[Array[Double]],Int))] =>
      val Tuple(idxs, vals, nItems) = in
      val rddIndexes = SRDDImpl(idxs)
      val rddValues = SRDDImpl(vals)
      val mR: Dataset1 = SparkSparseMatrix(RDDCollection(rddIndexes), RDDCollection(rddValues), nItems)
      mR.countNonZeroesByColumns.items.arr
    }

    lazy val reduceByColumnsFun = fun {in: Rep[(RDD[Array[Int]], (RDD[Array[Double]],Int))] =>
      val Tuple(idxs, vals, nItems) = in
      val rddIndexes = SRDDImpl(idxs)
      val rddValues = SRDDImpl(vals)
      val mR: Dataset1 = SparkSparseMatrix(RDDCollection(rddIndexes), RDDCollection(rddValues), nItems)
      mR.reduceByColumns.items.arr
    }

    lazy val transposeFun = fun {in: Rep[(RDD[Array[Int]], (RDD[Array[Double]],Int))] =>
      val Tuple(idxs, vals, nItems) = in
      val rddIndexes = SRDDImpl(idxs)
      val rddValues = SRDDImpl(vals)
      val mR: Dataset1 = SparkSparseMatrix(RDDCollection(rddIndexes), RDDCollection(rddValues), nItems)
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

  test("CF Code Gen") {

    val testCompiler = new SparkScalanCompiler with SparkLADslExp with BackendLASparkTests with CFDslExp {
      self =>
      val sparkContext = globalSparkContext
      val sSparkContext = null //ExpSSparkContextImpl(globalSparkContext)
      val conf = null //SSparkConf().setAppName(toRep("MVMTests")).setMaster(toRep("local[8]"))
      val repSparkContext = null //SSparkContext(conf)
    }

    val compiled0 = compileSource(testCompiler)(testCompiler.makeNewRDD, "makeNewRDD", generationConfig(testCompiler, "makeNewRDD", "package"))
    val compiled1 = compileSource(testCompiler)(testCompiler.flatMapFun, "flatMapFun", generationConfig(testCompiler, "flatMapFun", "package"))
    val compiled2 = compileSource(testCompiler)(testCompiler.avFun, "avFun", generationConfig(testCompiler, "avFun", "package"))
    val compiled3 = compileSource(testCompiler)(testCompiler.countNonZerosFun, "countNonZeros", generationConfig(testCompiler, "countNonZeros", "package"))
    val compiled4 = compileSource(testCompiler)(testCompiler.reduceByColumnsFun, "reduceByColumns", generationConfig(testCompiler, "reduceByColumns", "package"))
    val compiled5 = compileSource(testCompiler)(testCompiler.trainBL, "trainBL", generationConfig(testCompiler, "trainBL", "package"))
    val compiled6 = compileSource(testCompiler)(testCompiler.trainAndTestBL, "trainAndTestBL", generationConfig(testCompiler, "trainAndTestBL", "package"))
    val compiled7 = compileSource(testCompiler)(testCompiler.transposeFun, "transposeFun", generationConfig(testCompiler, "transposeFun", "package"))

  }

}


