package scalan.spark

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.scalatest.BeforeAndAfterAll
import scala.language.reflectiveCalls
import scalan._
import scalan.spark.collections.{RDDCollectionsDslExp, RDDCollectionsDslSeq, RDDCollectionsDsl}
import scalan.it.BaseCtxItTests

class SmokeTests extends BaseCtxItTests with BeforeAndAfterAll { suite =>
  val globalSparkConf = new SparkConf().setAppName("R/W Broadcast").setMaster("local")
  var globalSparkContext: SparkContext = null

  trait SimpleSparkTests extends ScalanDsl with SparkDsl with RDDCollectionsDsl {
    val prefix = suite.prefix
    val subfolder = "simple"

    lazy val broadcastPi = fun { (sc: Rep[SSparkContext]) => sc.broadcast(toRep(3.14)) }
    lazy val readE = fun { (sc: Rep[SSparkContext]) => {
      val be = sc.broadcast(toRep(2.71828))
      be.value
    }}
    lazy val broadcastDouble = fun { (in: Rep[(SSparkContext, Double)]) => {
      val Pair(sc, d) = in
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
    lazy val mapRDDColl = fun { (in: Rep[(SRDD[Double], Double)]) => {
      val Pair(rdd, i) = in
      val rddColl = RDDCollection(rdd)
      val resColl = rddColl.map(v => v + i)
      resColl.convertTo[IRDDCollection[Double]].rdd
    }}

    lazy val reduceRDDColl = fun { (rdd: RepRDD[Double]) => {
      val res = RDDCollection(rdd).reduce
      res
    }}

  }

  test("simpleSparkStaged") {
    val ctx = new TestContext("simpleSparkStaged") with SimpleSparkTests with SparkDslExp with RDDCollectionsDslExp {
      val sparkContext = globalSparkContext
      val sSparkContext = ExpSSparkContextImpl(globalSparkContext)
      val repSparkContext = SSparkContext(SSparkConf())
    }

    ctx.emit("broadcastPi", ctx.broadcastPi)
    ctx.emit("readE", ctx.readE)
    ctx.emit("broadcastDouble", ctx.broadcastDouble)
    ctx.emit("emptyRDD", ctx.emptyRDD)
    ctx.emit("mapRDD", ctx.mapRDD)
    ctx.emit("mapRDDColl", ctx.mapRDDColl)
  }
  test("simpleSparkCollectionsStaged") {
    val ctx = new TestContext("simpleCollectionSparkStaged") with SimpleSparkTests with SparkDslExp with RDDCollectionsDslExp {
      val sparkContext = globalSparkContext
      val sSparkContext = ExpSSparkContextImpl(globalSparkContext)
      val repSparkContext = SSparkContext(SSparkConf())
    }

    ctx.emit("mapRDDColl", ctx.mapRDDColl)
    ctx.emit("reduceRDDColl", ctx.reduceRDDColl)
  }

  test("simpleSparkSeq") {
    val ctx = new ScalanCtxSeq with SimpleSparkTests with SparkDslSeq with RDDCollectionsDslSeq {
      val sparkContext = globalSparkContext
      val sSparkContext = SeqSSparkContextImpl(globalSparkContext)
      val repSparkContext = SSparkContext(SSparkConf().setMaster("local[4]").setAppName("broadcastDouble"))
    }
    {
      val gravityAcc = 9.8
      val res = ctx.broadcastDouble((ctx.repSparkContext, gravityAcc))
      assertResult(gravityAcc)(res)
    }
  }
}

