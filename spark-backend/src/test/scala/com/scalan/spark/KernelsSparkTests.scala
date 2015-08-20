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
import scalan.common.OverloadHack.Overloaded1
import scalan.spark.SparkDslExp

trait KernelsSpark {

}

class KernelsSparkTests extends BaseTests with BeforeAndAfterAll with ItTestsUtil {

  val pref = new File("test-out/scalan/spark/backend/")
  val globalSparkConf = null
  var globalSparkContext: SparkContext = null

  def generationConfig[ScalanCake <: SparkDslExp](cmpl: SparkScalanCompiler[ScalanCake], pack : String = "gen",
                                                  command: String = null, getOutput: Boolean = false) =
    cmpl.defaultCompilerConfig.copy(scalaVersion = Some("2.10.4"),
      sbt = cmpl.defaultCompilerConfig.sbt.copy(mainPack = Some(s"com.scalan.spark.$pack"),
        resources = Seq("log4j.properties"),
        extraClasses = Seq("com.scalan.spark.method.Methods"),
        toSystemOut = !getOutput,
        commands = if (command == null) cmpl.defaultCompilerConfig.sbt.commands else Seq(command)))

  trait Functions extends SparkLADsl {

    //concrete types for specializations
    type DV = DenseVector[Double]
    type SV = SparseVector[Double]
    type CV = SparseVector1[Double]
    type DM = SparkDenseIndexedMatrix[Double]
    type SM = SparkSparseIndexedMatrix[Double]

    def vvm[T: Elem: Numeric](vA: Vector[T], vB: Vector[T]) = vA dot vB
    //def mvm[T: Numeric](mA: Matrix[T], vB: Vector[T]) = mA * vB
    def mvm[T: Elem: Numeric](m: Matrix[T], v: Vector[T]) = DenseVector(m.rows.map(r => r dot v))
    //def mmm[T: Numeric](mA: Matrix[T], mB: Matrix[T]) = mA * mB
    def mmm[T: Elem: Numeric](mA: Matrix[T], mB: Matrix[T]) = mA.map(row => DenseVector(mB.columns.map(col => row dot col)))
  }

  trait Specs_MVM extends Functions {

    def dddmvmDD(vs: (Rep[DM], Rep[DV])) = mvm(vs._1.convertTo[DM], vs._2.convertTo[DV])
    def dddmvmDS(vs: (Rep[DM], Rep[DV])) = mvm(vs._1.convertTo[DM], vs._2.convertTo[SV])
    def dddmvmDC(vs: (Rep[DM], Rep[DV])) = mvm(vs._1.convertTo[DM], vs._2.convertTo[CV])
    def dddmvmSD(vs: (Rep[DM], Rep[DV])) = mvm(vs._1.convertTo[SM], vs._2.convertTo[DV])
    def dddmvmSC(vs: (Rep[DM], Rep[DV])) = mvm(vs._1.convertTo[SM], vs._2.convertTo[CV])
    def dddmvmSS(vs: (Rep[DM], Rep[DV])) = mvm(vs._1.convertTo[SM], vs._2.convertTo[SV])

    def dsdmvmDD(vs: (Rep[DM], Rep[SV])) = mvm(vs._1.convertTo[DM], vs._2.convertTo[DV])
    def dsdmvmDS(vs: (Rep[DM], Rep[SV])) = mvm(vs._1.convertTo[DM], vs._2.convertTo[SV])
    def dsdmvmDC(vs: (Rep[DM], Rep[SV])) = mvm(vs._1.convertTo[DM], vs._2.convertTo[CV])
    def dsdmvmSD(vs: (Rep[DM], Rep[SV])) = mvm(vs._1.convertTo[SM], vs._2.convertTo[DV])
    def dsdmvmSC(vs: (Rep[DM], Rep[SV])) = mvm(vs._1.convertTo[SM], vs._2.convertTo[CV])
    def dsdmvmSS(vs: (Rep[DM], Rep[SV])) = mvm(vs._1.convertTo[SM], vs._2.convertTo[SV])

    def wrap(x: Rep[(SRDD[(Long, Array[Double])], Array[Double])])(implicit o: Overloaded1) = {
      val Tuple(rdd, b) = x
      val matrix: Rep[SparkDenseIndexedMatrix[Double]] = SparkDenseIndexedMatrix(RDDIndexedCollection(rdd), b.length)
      val vector: Rep[DenseVector[Double]] = DenseVector(Collection(b))
      (matrix, vector)
    }
    def wrap(x: Rep[(SRDD[(Long, Array[Double])], (Array[Int], (Array[Double], Int)))]) = {
      val Tuple(rdd, bi, bv, n) = x
      val matrix: Rep[SparkDenseIndexedMatrix[Double]] = SparkDenseIndexedMatrix(RDDIndexedCollection(rdd), n)
      val vector: Rep[SparseVector[Double]] = SparseVector(Collection(bi), Collection(bv), n)
      (matrix, vector)
    }
    def unwrap(y: Rep[DenseVector[Double]]) = isoC.from(isoD.from(y).convertTo(isoC.eTo))
    lazy val isoD = getIsoByElem(element[DenseVector[Double]]).asInstanceOf[Iso[Collection[Double], DenseVector[Double]]]
    lazy val isoC = getIsoByElem(element[CollectionOverArray[Double]]).asInstanceOf[Iso[Array[Double], CollectionOverArray[Double]]]
  }

  trait SparkMVM extends Specs_MVM {

    lazy val dddmvm_dd = fun { x: Rep[(SRDD[(Long, Array[Double])], Array[Double])] => unwrap(dddmvmDD(wrap(x))) }
    lazy val dddmvm_ds = fun { x: Rep[(SRDD[(Long, Array[Double])], Array[Double])] => unwrap(dddmvmDS(wrap(x))) }
    lazy val dddmvm_dc = fun { x: Rep[(SRDD[(Long, Array[Double])], Array[Double])] => unwrap(dddmvmDC(wrap(x))) }
    lazy val dddmvm_sd = fun { x: Rep[(SRDD[(Long, Array[Double])], Array[Double])] => unwrap(dddmvmSD(wrap(x))) }
    lazy val dddmvm_ss = fun { x: Rep[(SRDD[(Long, Array[Double])], Array[Double])] => unwrap(dddmvmSS(wrap(x))) }
    lazy val dddmvm_sc = fun { x: Rep[(SRDD[(Long, Array[Double])], Array[Double])] => unwrap(dddmvmSC(wrap(x))) }
    lazy val dsdmvm_dd = fun { x: Rep[(SRDD[(Long, Array[Double])], (Array[Int], (Array[Double], Int)))] => unwrap(dsdmvmDD(wrap(x))) }
    lazy val dsdmvm_ds = fun { x: Rep[(SRDD[(Long, Array[Double])], (Array[Int], (Array[Double], Int)))] => unwrap(dsdmvmDS(wrap(x))) }
    lazy val dsdmvm_dc = fun { x: Rep[(SRDD[(Long, Array[Double])], (Array[Int], (Array[Double], Int)))] => unwrap(dsdmvmDC(wrap(x))) }
    lazy val dsdmvm_sd = fun { x: Rep[(SRDD[(Long, Array[Double])], (Array[Int], (Array[Double], Int)))] => unwrap(dsdmvmSD(wrap(x))) }
    lazy val dsdmvm_ss = fun { x: Rep[(SRDD[(Long, Array[Double])], (Array[Int], (Array[Double], Int)))] => unwrap(dsdmvmSS(wrap(x))) }
    lazy val dsdmvm_sc = fun { x: Rep[(SRDD[(Long, Array[Double])], (Array[Int], (Array[Double], Int)))] => unwrap(dsdmvmSC(wrap(x))) }
  }

  class Program extends SparkMVM with SparkLADslExp {

    val sparkContext = globalSparkContext
    val sSparkContext = ExpSSparkContextImpl(globalSparkContext)
    val repSparkContext = SSparkContext(SSparkConf())
  }

  class TestClass[ScalanCake <: SparkDslExp](_scalan: ScalanCake) extends SparkScalanCompiler[ScalanCake](_scalan) {
    val sparkContext = globalSparkContext
    val sSparkContext = null
    val conf = null
    val repSparkContext = null
    override def graphPasses(config: CompilerConfig) = {
      Seq(AllUnpackEnabler, AllInvokeEnabler, CacheAndFusion, AllWrappersCleaner)
    }
  }

  test("dddmvm_dd Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.dddmvm_dd, "dddmvm_dd", generationConfig(testCompiler, "dddmvm_dd", "package"))
  }

  test("dddmvm_ds Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.dddmvm_ds, "dddmvm_ds", generationConfig(testCompiler, "dddmvm_ds", "package"))
  }

  test("dddmvm_sd Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.dddmvm_sd, "dddmvm_sd", generationConfig(testCompiler, "dddmvm_sd", "package"))
  }

  test("dddmvm_ss Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.dddmvm_ss, "dddmvm_ss", generationConfig(testCompiler, "dddmvm_ss", "package"))
  }

  test("dsdmvm_dd Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.dsdmvm_dd, "dsdmvm_dd", generationConfig(testCompiler, "dsdmvm_dd", "package"))
  }

  test("dsdmvm_ds Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.dsdmvm_ds, "dsdmvm_ds", generationConfig(testCompiler, "dsdmvm_ds", "package"))
  }

  test("dsdmvm_sd Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.dsdmvm_sd, "dsdmvm_sd", generationConfig(testCompiler, "dsdmvm_sd", "package"))
  }

  test("dsdmvm_ss Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.dsdmvm_ss, "dsdmvm_ss", generationConfig(testCompiler, "dsdmvm_ss", "package"))
  }
}
