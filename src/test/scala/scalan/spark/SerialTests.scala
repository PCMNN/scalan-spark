package scalan.spark

import java.io.File
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.scalatest.BeforeAndAfterAll
import scala.language.reflectiveCalls
import scalan._

class SerialTests extends BaseTests with BeforeAndAfterAll { suite =>
  val prefix = new File("test-out/scalan/spark/")
  val globalSparkConf = new SparkConf().setAppName("Serialization Tests").setMaster("local")
  var globalSparkContext: SparkContext = null

  override def beforeAll() = {
    globalSparkContext = new SparkContext(globalSparkConf)
  }

  override def afterAll() = {
    globalSparkContext.stop()
  }

  trait SimpleSerialTests extends ScalanDsl with SparkDsl {
    val prefix = suite.prefix
    val subfolder = "serial"

    lazy val plusOne = fun { (in: Rep[(SparkContext, Int)]) => {
      val Pair(sc, i: Rep[Int]) = in
      val rdd = sc.makeRDD(List.replicate(1, i))
      val incRdd = rdd.map(fun {v => v + 1})
      val result: Rep[Int] = incRdd.first

      result
    }}
  }

  ignore("simpleSerialSparkStaged") {
    val ctx = new TestContext with SimpleSerialTests with SparkDslExp {
      val sparkContext = globalSparkContext
    }

    ctx.emit("plusOne", ctx.plusOne)
  }

  ignore("simpleSerialSparkSeq") {
    val ctx = new ScalanCtxSeq with SimpleSerialTests with SparkDslSeq {
      val sparkContext = globalSparkContext
    }

    {
      val a = 42
      val res = ctx.plusOne((ctx.sparkContext, a))
      assertResult(a + 1)(res)
    }
  }

  test("serialSparkSeq") {
    class PlusOne(@transient val ctx: ScalanDsl with SparkDsl) extends Serializable {
      import ctx._

      def plusOne(in: Rep[(SparkContext, Int)]): Rep[Int] = {
        val Pair(sc, i: Rep[Int]) = in
        val rdd = sc.makeRDD(List.replicate(1, i))
        val incRdd = rdd.map(fun {v => v + 1})
        val result: Rep[Int] = incRdd.first

        result
      }
      //lazy val main = fun { (in: Rep[(SparkContext, Int)]) => plusOne(in)}
    }
    val ctx = new ScalanCtxSeq with SparkDslSeq {
      val sparkContext = globalSparkContext
    }
    {
      val inc = new PlusOne(ctx)
      import inc.ctx._
      val a = 42
      val res = inc.plusOne((sparkContext, a))
      assertResult(a + 1)(res)
    }
  }
}

