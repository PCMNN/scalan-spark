package com.scalan.spark

import java.io.File

import com.scalan.spark.backend.SparkScalanCompiler
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.scalatest.BeforeAndAfterAll

import scala.language.reflectiveCalls
import scalan.it.ItTestsUtil
import scalan.spark.SparkDsl
import scalan.{BaseTests, ScalanDsl}

class BackendTests extends BaseTests with BeforeAndAfterAll with ItTestsUtil { suite =>
  val pref = new File("test-out/scalan/spark/backend/")
  val globalSparkConf = new SparkConf().setAppName("R/W Broadcast").setMaster("local")
  var globalSparkContext: SparkContext = null

  override def beforeAll() = {
    globalSparkContext = new SparkContext(globalSparkConf)
  }

  override def afterAll() = {
    globalSparkContext.stop()
  }

  trait BackendSparkTests extends ScalanDsl with SparkDsl {
    val prefix = suite.pref
    val subfolder = "simple"
    lazy val sparkContextElem = element[SparkContext]
    lazy val defaultSparkContextRep = sparkContextElem.defaultRepValue
    lazy val sparkConfElem = element[SparkConf]
    lazy val defaultSparkConfRep = sparkConfElem.defaultRepValue

    lazy val broadcastPi = fun { (sc: Rep[SparkContext]) => sc.broadcast(toRep(3.14)) }

    lazy val readE = fun { (sc: Rep[SparkContext]) => {
      val be = sc.broadcast(toRep(2.71828))
      be.value
    }}

    lazy val broadcastDouble = fun { (in: Rep[(SparkContext, Double)]) => {
      val Pair(sc, d) = in
      val bd = sc.broadcast(d)
      bd.value
    }}

    lazy val emptyRDD = fun { (sc: Rep[SparkContext]) => {
      val rdd: Rep[RDD[Double]] = sc.emptyRDD
      rdd
    }}
    lazy val mapRDD = fun { (in: Rep[(RDD[Double], Double)]) => {
      val Pair(rdd, i) = in
      rdd.map(fun {v => v + i})
    }}
  }

  test("Broadcast Code Gen") {

    val testCompiler = new SparkScalanCompiler with BackendSparkTests {
      self =>
      val sparkContext = globalSparkContext
    }

    val res = getStagedOutputConfig(testCompiler)(testCompiler.broadcastPi, "broadcastPiCodeGen", globalSparkContext, testCompiler.defaultCompilerConfig.copy(scalaVersion = Some("2.11.4")))
  }

}

