package com.scalan.spark

import com.scalan.spark.backend.SparkScalanCompiler
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.scalatest.BeforeAndAfterAll

import scalan.it.ItTestsUtil
import scalan.BaseTests
import la.SparkLADslExp

class CollectionSparkTests extends BaseTests with BeforeAndAfterAll with ItTestsUtil { suite =>
  trait CollectionSamplesExp extends SparkLADslExp {
    lazy val denseVector = fun { arr: Rep[Array[Double]] => DenseVector(Collection.fromArray(arr)) }
    lazy val sparkSparseIndexedMatrix = fun { in: Rep[(RDD[(Long, Array[Int])], (RDD[(Long, Array[Double])], Int))] =>
      val Tuple(idxsMatrixRDD, valsMatrixRDD, nColumnsMatrix) = in
      val rddCollIdxsMatrix = RDDIndexedCollection(SRDDImpl(idxsMatrixRDD))
      val rddCollValsMatrix = RDDIndexedCollection(SRDDImpl(valsMatrixRDD))
      SparkSparseIndexedMatrix(rddCollIdxsMatrix, rddCollValsMatrix, nColumnsMatrix)
    }

    lazy val spsmdv = fun { in: Rep[(RDD[(Long, Array[Int])], (RDD[(Long, Array[Double])], (Int, Array[Double])))] =>
      val Tuple(idxsMatrixRDD, valsMatrixRDD, nColumnsMatrix, vData) = in
      val m = sparkSparseIndexedMatrix(idxsMatrixRDD, valsMatrixRDD, nColumnsMatrix)
      val v = denseVector(vData)
      (m * v).items.arr
    }
    lazy val smReduceByColumns = fun { in: Rep[(RDD[(Long, Array[Int])], (RDD[(Long, Array[Double])], Int))] =>
      val Tuple(idxsMatrixRDD, valsMatrixRDD, nColumnsMatrix) = in
      val m = sparkSparseIndexedMatrix(idxsMatrixRDD, valsMatrixRDD, nColumnsMatrix)
      m.reduceByColumns.items.arr
    }
  }
  class ProgramExp extends CollectionSamplesExp {
    val sparkContext: SparkContext = null
    val sSparkContext = ExpSSparkContextImpl(sparkContext)
    val repSparkContext = SSparkContext(SSparkConf())
  }
  class TestCompiler[ScalanCake <: CollectionSamplesExp](program: ScalanCake) extends SparkScalanCompiler(program)
  def generationConfig[ScalanCake <: SparkLADslExp](cmpl: SparkScalanCompiler[ScalanCake], pack : String = "gen", command: String = null, getOutput: Boolean = false) =
    cmpl.defaultCompilerConfig.copy(scalaVersion = Some("2.10.4"),
      sbt = cmpl.defaultCompilerConfig.sbt.copy(mainPack = Some(s"com.scalan.spark.$pack"),
        resources = Seq("log4j.properties"),
        extraClasses = Seq("com.scalan.spark.method.Methods"),
        toSystemOut = !getOutput,
        commands = if (command == null) cmpl.defaultCompilerConfig.sbt.commands else Seq(command)))

  val testCompiler = new TestCompiler(new ProgramExp)
  import testCompiler.scalan._
  def commonCompileScenario[A: Elem, B:Elem](testFun: Rep[A => B], testName: String) =
    compileSource(testCompiler)(testFun, testName, generationConfig(testCompiler, testName, "package"))
  test("code gen - spsmdv") { val compiled = commonCompileScenario(spsmdv, "spsmdv") }
  test("code gen - smReduceByColumns") { val compiled = commonCompileScenario(smReduceByColumns, "smReduceByColumns") }
}