package scalan.spark

import java.io.File
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.scalatest.BeforeAndAfterAll
import scala.language.reflectiveCalls
import scalan._
import scalan.spark.collections.{RDDCollectionsDsl, RDDCollectionsDslSeq, RDDCollectionsDslExp}

class SerialTests extends BaseTests with BeforeAndAfterAll with TestContexts { suite =>
  val globalSparkConf = new SparkConf().setAppName("Serialization Tests").setMaster("local[4]")
  var globalSparkContext: SparkContext = null

  trait SimpleSerialTests extends ScalanDsl with SparkDsl with RDDCollectionsDsl {
    val prefix = suite.prefix
    val subfolder = "serial"

    lazy val plusOne = fun { (in: Rep[(SSparkContext, Int)]) => {
      val Pair(sc, i) = in
      val rdd = sc.makeRDD(SSeq.fromList(SList.replicate(1, i)))
      val incRdd = rdd.map(fun {v => v + 1})
      val result: Rep[Int] = incRdd.first

      result
    }}

    lazy val zipRdd = fun { (in: Rep[(SRDD[(Long, Int)], SRDD[(Long, Double)])]) =>
      val Pair(aRdd, bRdd) = in
      aRdd zip bRdd
    }
//    lazy val zipRddVector = fun { (in: Rep[(SRDD[(Long, Array[Int])], SRDD[(Long, Array[Double])])]) =>
//      val Pair(aRdd, bRdd) = in
//      val avRdd = aRdd.map { fun { p: Rep[(Long, Array[Int])] => (p._1, DenseVector(CollectionOverArray(p._2))) } }
//      val bvRdd = bRdd.map { fun { p: Rep[(Long, Array[Double])] => (p._1, DenseVector(CollectionOverArray(p._2))) } }
//      val zvRdd = avRdd zip bvRdd
//      zvRdd.map { zp: Rep[((Long, AbstractVector[Int]), (Long, AbstractVector[Double]))] => zp._1._1 + zp._2 }
//    }
    lazy val zipRddVector = fun { rdd: Rep[RDD[(Long, Array[Int])]] =>
      val res = RDDIndexedCollection(SRDDImpl(rdd))
//      res.map(rdd => rdd.map { p: Rep[(Long, Array[Int])] => p._2.map(x => x.toLong + p._1)}).arr
      res.map(rdd => rdd.map { p: Rep[Int] => p + 1 }).arr
    }
  }

  test("simpleSerialSparkStaged") {
    val ctx = new TestContext("simpleSerialSparkStaged") with SimpleSerialTests with SparkDslExp with RDDCollectionsDslExp {
      val sparkContext = globalSparkContext
      val sSparkContext = ExpSSparkContextImpl(globalSparkContext)
      val repSparkContext = SSparkContext(SSparkConf())
    }

    ctx.emit("plusOne", ctx.plusOne)
    ctx.emit("zipRdd", ctx.zipRdd)
  }

  test("zipRddVector") {
    val ctx = new TestContext("simpleSerialSparkStaged") with SimpleSerialTests with SparkDslExp with RDDCollectionsDslExp {
      val sparkContext = globalSparkContext
      val sSparkContext = ExpSSparkContextImpl(globalSparkContext)
      val repSparkContext = SSparkContext(SSparkConf())
    }
    ctx.emit("zipRddVector", ctx.zipRddVector)
  }

  ignore("simpleSerialSparkSeq") {
    val ctx = new ScalanCtxSeq with SimpleSerialTests with SparkDslSeq with RDDCollectionsDslSeq {
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

