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

class MMMSparkTests extends BaseTests with BeforeAndAfterAll with ItTestsUtil with KernelsSpark {

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

  class Program extends SparkMMM with SparkLADslExp {

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

  test("dddmmm_dd Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.dddmmm_dd, "dddmmm_dd", generationConfig(testCompiler, "dddmmm_dd", "package"))
  }
  test("dddmmm_ds Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.dddmmm_ds, "dddmmm_ds", generationConfig(testCompiler, "dddmmm_ds", "package"))
  }
  test("dddmmm_sd Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.dddmmm_sd, "dddmmm_sd", generationConfig(testCompiler, "dddmmm_sd", "package"))
  }
  test("dddmmm_ss Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.dddmmm_ss, "dddmmm_ss", generationConfig(testCompiler, "dddmmm_ss", "package"))
  }

  test("dsdmmm_dd Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.dsdmmm_dd, "dsdmmm_dd", generationConfig(testCompiler, "dsdmmm_dd", "package"))
  }
  test("dsdmmm_ds Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.dsdmmm_ds, "dsdmmm_ds", generationConfig(testCompiler, "dsdmmm_ds", "package"))
  }
  test("dsdmmm_sd Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.dsdmmm_sd, "dsdmmm_sd", generationConfig(testCompiler, "dsdmmm_sd", "package"))
  }
  test("dsdmmm_ss Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.dsdmmm_ss, "dsdmmm_ss", generationConfig(testCompiler, "dsdmmm_ss", "package"))
  }

  test("sddmmm_dd Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.sddmmm_dd, "sddmmm_dd", generationConfig(testCompiler, "sddmmm_dd", "package"))
  }
  test("sddmmm_ds Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.sddmmm_ds, "sddmmm_ds", generationConfig(testCompiler, "sddmmm_ds", "package"))
  }
  test("sddmmm_sd Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.sddmmm_sd, "sddmmm_sd", generationConfig(testCompiler, "sddmmm_sd", "package"))
  }
  test("sddmmm_ss Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.sddmmm_ss, "sddmmm_ss", generationConfig(testCompiler, "sddmmm_ss", "package"))
  }

  test("ssdmmm_dd Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.ssdmmm_dd, "ssdmmm_dd", generationConfig(testCompiler, "ssdmmm_dd", "package"))
  }
  test("ssdmmm_ds Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.ssdmmm_ds, "ssdmmm_ds", generationConfig(testCompiler, "ssdmmm_ds", "package"))
  }
  test("ssdmmm_sd Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.ssdmmm_sd, "ssdmmm_sd", generationConfig(testCompiler, "ssdmmm_sd", "package"))
  }
  test("ssdmmm_ss Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.ssdmmm_ss, "ssdmmm_ss", generationConfig(testCompiler, "ssdmmm_ss", "package"))
  }
}
