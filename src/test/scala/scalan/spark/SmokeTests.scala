package scalan.spark

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.scalatest.BeforeAndAfterAll
import scala.language.reflectiveCalls
import scalan._
import scalan.spark.collections.{RDDCollectionsDslExp, RDDCollectionsDslSeq, RDDCollectionsDsl}

class SmokeTests extends BaseTests with BeforeAndAfterAll { suite =>
  val globalSparkConf = new SparkConf().setAppName("R/W Broadcast").setMaster("local")
  var globalSparkContext: SparkContext = null

  override def beforeAll() = {
    globalSparkContext = new SparkContext(globalSparkConf)
  }

  override def afterAll() = {
    globalSparkContext.stop()
  }

  trait SimpleSparkTests extends ScalanDsl with SparkDsl with RDDCollectionsDsl {
    val prefix = suite.prefix
    val subfolder = "simple"
    lazy val sparkContextElem = element[SSparkContext]
    lazy val defaultSparkContextRep = sparkContextElem.defaultRepValue
    lazy val sparkConfElem = element[SparkConf]
    lazy val defaultSparkConfRep = sparkConfElem.defaultRepValue

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
      convertRDDColl(resColl).rdd
    }}

    lazy val reduceRDDColl = fun { (rdd: RepRDD[Double]) => {
      val res = RDDCollection(rdd).reduce
      res
    }}

  }

  /*test("simpleSparkStaged") {
    val ctx = new TestContext(this, "simpleSparkStaged") with SimpleSparkTests with SparkDslExp with RDDCollectionsDslExp {
      val sparkContext = globalSparkContext
      val sSparkContext = ExpSSparkContextImpl(globalSparkContext)
      val repSparkContext = SSparkContext(SSparkConf())
    }

    ctx.emit("defaultSparkContextRep", ctx.defaultSparkContextRep)
    ctx.emit("defaultSparkConfRep", ctx.defaultSparkConfRep)
    ctx.emit("broadcastPi", ctx.broadcastPi)
    ctx.emit("readE", ctx.readE)
    ctx.emit("broadcastDouble", ctx.broadcastDouble)
    ctx.emit("emptyRDD", ctx.emptyRDD)
    ctx.emit("mapRDD", ctx.mapRDD)
    ctx.emit("mapRDDColl", ctx.mapRDDColl)
  } */
  test("simpleSparkCollectionsStaged") {
    val ctx = new TestContext(this, "simpleCollectionSparkStaged") with SimpleSparkTests with SparkDslExp with RDDCollectionsDslExp {
      val sparkContext = globalSparkContext
      val sSparkContext = ExpSSparkContextImpl(globalSparkContext)
      val repSparkContext = SSparkContext(SSparkConf())
    }

    ctx.emit("mapRDDColl", ctx.mapRDDColl)
    ctx.emit("reduceRDDColl", ctx.reduceRDDColl)
  }
  /*
  test("simpleSparkSeq") {
    val ctx = new ScalanCtxSeq with SimpleSparkTests with SparkDslSeq with RDDCollectionsDslSeq {
      val sparkContext = globalSparkContext
      val sSparkContext = SeqSSparkContextImpl(globalSparkContext)
      val repSparkContext = SSparkContext(SSparkConf())
    }

    {
      val gravityAcc = 9.8
      val res = ctx.broadcastDouble((ctx.sSparkContext, gravityAcc))
      assertResult(gravityAcc)(res)
    }
  }  */
}

