/*
package com.scalan.spark

/**
 * Created by Victor Smirnov on 8/25/15.
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
import scalan.common.OverloadHack.{Overloaded3, Overloaded2, Overloaded1}
import scalan.spark.SparkDslExp

class MVMSparkTests extends BaseTests with BeforeAndAfterAll with ItTestsUtil with KernelsSpark {

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

  ignore("dddmvm_dd Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.dddmvm_dd, "dddmvm_dd", generationConfig(testCompiler, "dddmvm_dd", "package"))
  }
  ignore("dddmvm_ds Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.dddmvm_ds, "dddmvm_ds", generationConfig(testCompiler, "dddmvm_ds", "package"))
  }
  ignore("dddmvm_sd Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.dddmvm_sd, "dddmvm_sd", generationConfig(testCompiler, "dddmvm_sd", "package"))
  }
  ignore("dddmvm_ss Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.dddmvm_ss, "dddmvm_ss", generationConfig(testCompiler, "dddmvm_ss", "package"))
  }

  ignore("dsdmvm_dd Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.dsdmvm_dd, "dsdmvm_dd", generationConfig(testCompiler, "dsdmvm_dd", "package"))
  }
  ignore("dsdmvm_ds Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.dsdmvm_ds, "dsdmvm_ds", generationConfig(testCompiler, "dsdmvm_ds", "package"))
  }
  ignore("dsdmvm_sd Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.dsdmvm_sd, "dsdmvm_sd", generationConfig(testCompiler, "dsdmvm_sd", "package"))
  }
  ignore("dsdmvm_ss Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.dsdmvm_ss, "dsdmvm_ss", generationConfig(testCompiler, "dsdmvm_ss", "package"))
  }

  test("sddmvm_dd Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.sddmvm_dd, "sddmvm_dd", generationConfig(testCompiler, "sddmvm_dd", "package"))
  }
  test("sddmvm_ds Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.sddmvm_ds, "sddmvm_ds", generationConfig(testCompiler, "sddmvm_ds", "package"))
  }
  test("sddmvm_sd Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.sddmvm_sd, "sddmvm_sd", generationConfig(testCompiler, "sddmvm_sd", "package"))
  }
  test("sddmvm_ss Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.sddmvm_ss, "sddmvm_ss", generationConfig(testCompiler, "sddmvm_ss", "package"))
  }

  test("ssdmvm_dd Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.ssdmvm_dd, "ssdmvm_dd", generationConfig(testCompiler, "ssdmvm_dd", "package"))
  }
  test("ssdmvm_ds Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.ssdmvm_ds, "ssdmvm_ds", generationConfig(testCompiler, "ssdmvm_ds", "package"))
  }
  test("ssdmvm_sd Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.ssdmvm_sd, "ssdmvm_sd", generationConfig(testCompiler, "ssdmvm_sd", "package"))
  }
  test("ssdmvm_ss Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.ssdmvm_ss, "ssdmvm_ss", generationConfig(testCompiler, "ssdmvm_ss", "package"))
  }
}
*/
