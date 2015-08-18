package com.scalan.spark

/**
 * Created by Victor Smirnov on 7/27/15.
 */

import java.io.File

import com.scalan.spark.backend.SparkScalanCompiler
import la.{SparkLADsl, SparkLADslExp}
import org.apache.spark._
import org.scalatest.BeforeAndAfterAll

import scala.language.reflectiveCalls
import scalan.BaseTests
import scalan.it.ItTestsUtil
import scalan.la.LADsl

class KernelsSparkTests extends BaseTests with BeforeAndAfterAll with ItTestsUtil {

  val pref = new File("test-out/scalan/spark/backend/")
  val globalSparkConf = null
  var globalSparkContext: SparkContext = null

  def generationConfig(cmpl: SparkScalanCompiler, pack : String = "gen", command: String = null, getOutput: Boolean = false) =
    cmpl.defaultCompilerConfig.copy(scalaVersion = Some("2.10.4"),
      sbt = cmpl.defaultCompilerConfig.sbt.copy(mainPack = Some(s"com.scalan.spark.$pack"),
        resources = Seq("log4j.properties"),
        extraClasses = Seq("com.scalan.spark.method.Methods"),
        toSystemOut = !getOutput,
        commands = if (command == null) cmpl.defaultCompilerConfig.sbt.commands else Seq(command)))

  trait Functions extends SparkLADsl {

    def vvm[T: Elem: Numeric](vA: Vector[T], vB: Vector[T]) = vA dot vB
    //def mvm[T: Numeric](mA: Matrix[T], vB: Vector[T]) = mA * vB
    def mvm[T: Elem: Numeric](m: Matrix[T], v: Vector[T]) = DenseVector(m.rows.map(r => r dot v))
    //def mmm[T: Numeric](mA: Matrix[T], mB: Matrix[T]) = mA * mB
    def mmm[T: Elem: Numeric](mA: Matrix[T], mB: Matrix[T]) = mA.map(row => DenseVector(mB.columns.map(col => row dot col)))
  }

  trait Specs_MVM extends Functions {

    type DV = DenseVector[Double]
    type SV = SparseVector[Double]
    type CV = SparseVector1[Double]

    type DM = SparkDenseIndexedMatrix[Double]
    type SM = SparkSparseIndexedMatrix[Double]

    def specDMDV(vs: (Rep[SparkDenseIndexedMatrix[Double]], Rep[DenseVector[Double]])) = mvm(vs._1.convertTo[DM], vs._2.convertTo[DV])
    def specDMSV(vs: (Rep[SparkDenseIndexedMatrix[Double]], Rep[DenseVector[Double]])) = mvm(vs._1.convertTo[DM], vs._2.convertTo[SV])
    def specDMCV(vs: (Rep[SparkDenseIndexedMatrix[Double]], Rep[DenseVector[Double]])) = mvm(vs._1.convertTo[DM], vs._2.convertTo[CV])
    def specSMDV(vs: (Rep[SparkDenseIndexedMatrix[Double]], Rep[DenseVector[Double]])) = {
      val a = vs._1
      val b = vs._2
      val mA = a.convertTo[SM]
      val vB = b.convertTo[DV]
      mvm(mA, vB)
      //mvm(vs._1.convertTo[SM], vs._2.convertTo[DV])
    }
    def specSMCV(vs: (Rep[SparkDenseIndexedMatrix[Double]], Rep[DenseVector[Double]])) = mvm(vs._1.convertTo[SM], vs._2.convertTo[SV])
    def specSMSV(vs: (Rep[SparkDenseIndexedMatrix[Double]], Rep[DenseVector[Double]])) = mvm(vs._1.convertTo[SM], vs._2.convertTo[CV])
    def wrap(x: Rep[(SRDD[(Long, Array[Double])], Array[Double])]) = {
      val Tuple(rdd, b) = x
      val matrix: Rep[SparkDenseIndexedMatrix[Double]] = SparkDenseIndexedMatrix(RDDIndexedCollection(rdd), b.length)
      val vector: Rep[DenseVector[Double]] = DenseVector(Collection(b))
      (matrix, vector)
    }
    def unwrap(y: Rep[DenseVector[Double]]) = isoC.from(isoD.from(y).convertTo(isoC.eTo))
    lazy val isoD = getIsoByElem(element[DenseVector[Double]]).asInstanceOf[Iso[Collection[Double], DenseVector[Double]]]
    lazy val isoC = getIsoByElem(element[CollectionOverArray[Double]]).asInstanceOf[Iso[Array[Double], CollectionOverArray[Double]]]
  }

  trait KernelsSpark extends Specs_MVM {

  lazy val ddmvm_dd = fun { x: Rep[(SRDD[(Long, Array[Double])], Array[Double])] => unwrap(specDMDV(wrap(x))) }
  lazy val ddmvm_ds = fun { x: Rep[(SRDD[(Long, Array[Double])], Array[Double])] => unwrap(specDMSV(wrap(x))) }
  lazy val ddmvm_dc = fun { x: Rep[(SRDD[(Long, Array[Double])], Array[Double])] => unwrap(specDMCV(wrap(x))) }
  lazy val ddmvm_sd = fun { x: Rep[(SRDD[(Long, Array[Double])], Array[Double])] => unwrap(specSMDV(wrap(x))) }
  lazy val ddmvm_ss = fun { x: Rep[(SRDD[(Long, Array[Double])], Array[Double])] => unwrap(specSMSV(wrap(x))) }
  lazy val ddmvm_sc = fun { x: Rep[(SRDD[(Long, Array[Double])], Array[Double])] => unwrap(specSMCV(wrap(x))) }
}

  class TestClass extends SparkScalanCompiler with SparkLADslExp with KernelsSpark {
    val sparkContext = globalSparkContext
    val sSparkContext = null
    val conf = null
    val repSparkContext = null
    override def graphPasses(config: CompilerConfig) = {
      Seq(AllUnpackEnabler, AllInvokeEnabler, CacheAndFusion, AllWrappersCleaner)
    }
  }

  ignore("ddmvm_dd Code Gen") {
    val testCompiler = new TestClass
    val compiled1 = compileSource(testCompiler)(testCompiler.ddmvm_dd, "ddmvm_dd", generationConfig(testCompiler, "ddmvm_dd", "package"))
  }

  ignore("ddmvm_ds Code Gen") {
    val testCompiler = new TestClass
    compileSource(testCompiler)(testCompiler.ddmvm_ds, "ddmvm_ds", generationConfig(testCompiler, "ddmvm_ds", "package"))
  }

  test("ddmvm_sd Code Gen") {
    val testCompiler = new TestClass
    compileSource(testCompiler)(testCompiler.ddmvm_sd, "ddmvm_sd", generationConfig(testCompiler, "ddmvm_sd", "package"))
  }

  test("ddmvm_ss Code Gen") {
    val testCompiler = new TestClass
    compileSource(testCompiler)(testCompiler.ddmvm_ss, "ddmvm_ss", generationConfig(testCompiler, "ddmvm_ss", "package"))
  }
}
