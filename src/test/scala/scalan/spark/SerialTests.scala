package scalan.spark

import java.io.File
import org.apache.spark._
import org.scalatest.BeforeAndAfterAll
import scala.language.reflectiveCalls
import scalan._

class SerialTests extends BaseTests with BeforeAndAfterAll with TestContexts { suite =>
  val globalSparkConf = new SparkConf().setAppName("Serialization Tests").setMaster("local[4]")
  var globalSparkContext: SparkContext = null

  trait SimpleSerialTests extends ScalanDsl with SparkDsl {
    val prefix = suite.prefix
    val subfolder = "serial"

    lazy val plusOne = fun { (in: Rep[(SSparkContext, Int)]) => {
      val Pair(sc, i) = in
      val rdd = sc.makeRDD(SSeq.fromList(SList.replicate(1, i)))
      val incRdd = rdd.map(fun {v => v + 1})
      val result: Rep[Int] = incRdd.first

      result
    }}
  }

  test("simpleSerialSparkStaged") {
    val ctx = new TestContext("simpleSerialSparkStaged") with SimpleSerialTests with SparkDslExp {
      val sparkContext = globalSparkContext
      val sSparkContext = ExpSSparkContextImpl(globalSparkContext)
      val repSparkContext = SSparkContext(SSparkConf())
    }

    ctx.emit("plusOne", ctx.plusOne)
  }

  ignore("simpleSerialSparkSeq") {
    val ctx = new ScalanCtxSeq with SimpleSerialTests with SparkDslSeq {
      val sparkContext = globalSparkContext
      val sSparkContext = SeqSSparkContextImpl(globalSparkContext)
      val repSparkContext = SSparkContext(SSparkConf())
    }

    {
      val a = 42
      val res = ctx.plusOne((ctx.sSparkContext, a))
      assertResult(a + 1)(res)
    }
  }

  ignore("serialSparkSeq") { // should be passed?
    class PlusOne(@transient val ctx: ScalanDsl with SparkDsl) extends Serializable {
      import ctx._

      def plusOne(in: Rep[(SSparkContext, Int)]): Rep[Int] = {
        val Pair(sc, i) = in
        val rdd = sc.makeRDD(SSeq.fromList(SList.replicate(1, i)))
        val incRdd = rdd.map(fun {v => v + 1} )
        val result: Rep[Int] = incRdd.first

        result
      }
      //lazy val main = fun { (in: Rep[(SparkContext, Int)]) => plusOne(in)}
    }
    val ctx = new ScalanCtxSeq with SparkDslSeq {
      val sparkContext = globalSparkContext
      val sSparkContext = SeqSSparkContextImpl(globalSparkContext)
      val repSparkContext = SSparkContext(SSparkConf().setMaster("local[4]").setAppName("pluOne"))
    }
    {
      val inc = new PlusOne(ctx)
      import inc.ctx._
      val a = 42
      val res = inc.plusOne(Pair(inc.ctx.repSparkContext, a))
      assertResult(a + 1)(res)
    }
  }
}

