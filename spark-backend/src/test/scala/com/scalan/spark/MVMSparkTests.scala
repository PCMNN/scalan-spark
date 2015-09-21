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

  test("smdm converter Code Gen") {
    val testCompiler = new TestClass(new Program)
    compileSource(testCompiler)(testCompiler.scalan.smdmsv_conv, "smdmsv_conv", generationConfig(testCompiler, "smdmsv_conv", "package"))
  }
}
*/
