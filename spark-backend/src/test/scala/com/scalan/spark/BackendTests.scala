package com.scalan.spark

import java.io.File

import com.scalan.spark.backend.SparkScalanCompiler
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.scalatest.BeforeAndAfterAll

import scala.language.reflectiveCalls
import scalan.it.ItTestsUtil
import scalan.spark.{SparkDslExp, SparkDsl}
import scalan.{BaseTests, ScalanDsl}

class BackendTests extends BaseTests with BeforeAndAfterAll with ItTestsUtil { suite =>
  val pref = new File("test-out/scalan/spark/backend/")
  val globalSparkConf = null //new SparkConf().setAppName("R/W Broadcast").setMaster("local")
  var globalSparkContext: SparkContext = null

  override def beforeAll() = {
    //globalSparkContext = new SparkContext(globalSparkConf)
  }

  override def afterAll() = {
    //globalSparkContext.stop()
  }

  trait BackendSparkTests extends ScalanDsl with SparkDsl {
    val prefix = suite.pref
    val subfolder = "simple"
    lazy val sparkContextElem = element[SparkContext]
    lazy val defaultSparkContextRep = sparkContextElem.defaultRepValue
    lazy val sparkConfElem = element[SparkConf]
    lazy val defaultSparkConfRep = sparkConfElem.defaultRepValue

    lazy val broadcastPi = fun { (sc: Rep[SSparkContext]) =>
      sc.broadcast(toRep(3.14))
    }

    lazy val readE = fun { (sc: Rep[SSparkContext]) => {
      val be = sc.broadcast(toRep(2.71828))
      be.value
    }}

    lazy val broadcastDouble = fun { in: Rep[(SSparkContext,Double)] => {
      val Pair(sc,d) = in
      val bd = sc.broadcast(d)
      bd.value
    }}

    lazy val emptyRDD = fun { (sc: Rep[SSparkContext]) => {
      val rdd: Rep[SRDD[Double]] = sc.emptyRDD
      rdd
    }}
    lazy val mapRDD = fun { (in: Rep[(SRDD[Double], Double)]) => {
      val Pair(rdd, i) = in
      rdd.map(fun {v => v + i})
    }}

    lazy val newRDD = fun { (in: Rep[Int]) => {
      val rdd: Rep[SRDD[Int]] = repSparkContext.makeRDD(SSeq.single(in),2)
      val res = rdd.collect
      (repSparkContext.stop | res)
    }}
  }

  def generationConfig[ScalanCake <: SparkDslExp](cmpl: SparkScalanCompiler[ScalanCake], pack : String = "gen", command: String = null, getOutput: Boolean = false) =
    cmpl.defaultCompilerConfig.copy(scalaVersion = Some("2.10.4"),
      sbt = cmpl.defaultCompilerConfig.sbt.copy(mainPack = Some(s"com.scalan.spark.$pack"),
        resources = Seq("log4j.properties"),
        extraClasses = Seq("com.scalan.spark.method.Methods"),
        toSystemOut = !getOutput,
        commands = if (command == null) cmpl.defaultCompilerConfig.sbt.commands else Seq(command)))

  class Program extends BackendSparkTests with SparkDslExp {

    val sparkContext = globalSparkContext
    val sSparkContext = ExpSSparkContextImpl(globalSparkContext)
    val repSparkContext = SSparkContext(SSparkConf())
  }
  test("Broadcast/RDD Code Gen") {

    val testCompiler = new SparkScalanCompiler(new Program) {
      import scalan._
      val sparkContext = globalSparkContext
      val sSparkContext = ExpSSparkContextImpl(globalSparkContext)
      val conf = SSparkConf().setAppName(toRep("BTests")).setMaster(toRep("local[8]"))
      val repSparkContext: Rep[SSparkContext] = SSparkContext(conf)
    }

    val compiled = compileSource(testCompiler)(testCompiler.scalan.broadcastPi, "broadcastPi", generationConfig(testCompiler, "broadcastPi", "package"))
    val compiled1 = compileSource(testCompiler)(testCompiler.scalan.broadcastDouble, "broadcastDouble", generationConfig(testCompiler, "broadcastDouble", "package"))
    val compiled2 = compileSource(testCompiler)(testCompiler.scalan.readE, "readE", generationConfig(testCompiler, "readE", "package"))
    val compiled3 = compileSource(testCompiler)(testCompiler.scalan.emptyRDD, "emptyRDD", generationConfig(testCompiler, "emptyRDD", "package"))
    val compiled4 = compileSource(testCompiler)(testCompiler.scalan.mapRDD, "mapRDD", generationConfig(testCompiler, "mapRDD", "package"))
    val compiled5 = compileSource(testCompiler)(testCompiler.scalan.newRDD, "newRDD", generationConfig(testCompiler, "newRDD", "package"))
  }

}

