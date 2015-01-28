package scalan.spark

import java.io.File
import org.apache.spark._
import org.apache.spark.rdd.RDD
import scala.language.reflectiveCalls
import scalan._

class SmokeTests extends BaseTests { suite =>
  val prefix = new File("test-out/scalan/spark/")

  trait SimpleSparkTests extends ScalanDsl with SparkDsl {
    val prefix = suite.prefix
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

  test("simpleSparkStaged") {
    val ctx = new TestContext with SimpleSparkTests with SparkDslExp {
      def print(s: Rep[String]): Rep[Unit] = print(s)
      def read: Rep[String] = Console.readLine()
      val sparkConf = new SparkConf().setAppName("R/W Broadcast").setMaster("local")
      val sparkContext = new SparkContext(sparkConf)
    }

    ctx.emit("defaultSparkContextRep", ctx.defaultSparkContextRep)
    ctx.emit("defaultSparkConfRep", ctx.defaultSparkConfRep)
    ctx.emit("broadcastPi", ctx.broadcastPi)
    ctx.emit("readE", ctx.readE)
    //ctx.emit("broadcastDouble", ctx.broadcastDouble)
    ctx.emit("emptyRDD", ctx.emptyRDD)
    ctx.emit("mapRDD", ctx.mapRDD)
  }

  test("simpleSparkSeq") {
    val ctx = new ScalanCtxSeq with SimpleSparkTests with SparkDslSeq {
      val sparkConf = new SparkConf().setAppName("R/W Broadcast").setMaster("local")
      val sparkContext = new SparkContext(sparkConf)
    }

    {
      val gravityAcc = 9.8
      val res = ctx.broadcastDouble((ctx.sparkContext, gravityAcc))
      assertResult(gravityAcc)(res)
    }
  }
}

