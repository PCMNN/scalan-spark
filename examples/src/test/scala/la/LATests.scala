/**
 * Created by afilippov on 3/30/15.
 */
package la

import org.apache.spark._
import org.scalatest.BeforeAndAfterAll

import scala.language.reflectiveCalls
import scalan.la.{LADslExp, LADsl}
import scalan.{BaseTests, ScalanDsl, TestContext}
import scalan.spark.collections.{RDDCollectionsDsl, RDDCollectionsDslExp}
import scalan.spark.{SparkDsl, SparkDslExp}

class LATests extends BaseTests with BeforeAndAfterAll { suite =>
  val globalSparkConf = new SparkConf().setAppName("R/W Broadcast").setMaster("local")
  var globalSparkContext: SparkContext = null

  override def beforeAll() = {
    globalSparkContext = new SparkContext(globalSparkConf)
  }

  override def afterAll() = {
    globalSparkContext.stop()
  }

  trait SimpleLASparkTests extends ScalanDsl with SparkDsl with RDDCollectionsDsl with LADsl {
    val prefix = suite.prefix
    val subfolder = "simple"
    lazy val sparkContextElem = element[SSparkContext]
    lazy val defaultSparkContextRep = sparkContextElem.defaultRepValue
    lazy val sparkConfElem = element[SparkConf]
    lazy val defaultSparkConfRep = sparkConfElem.defaultRepValue


    def dvDotDV(in1: Coll[Int], in2: Coll[Int]) = {
      val (vector1, vector2): (Vector[Int], Vector[Int]) = (DenseVector(in1), DenseVector(in2))
      vector1 dot vector2
    }

    def svDotDV(in1_1: Coll[Int], in1_2: Coll[Int], in2: Coll[Int]) = {
      val vector1: Vector[Int] = SparseVector(in1_1, in1_2, in2.length)
      val vector2: Vector[Int] = DenseVector(in2)
      vector1 dot vector2
    }

    lazy val funDvDotDv = fun { in: Rep[(SRDD[Int], SRDD[Int])] =>
      val in1 = RDDCollection(in._1)
      val in2 = RDDCollection(in._2)
      dvDotDV(in1, in2)
    }

    lazy val funSvDotDv = fun { in: Rep[((SRDD[Int], SRDD[Int]), SRDD[Int])] =>
      val in1_1 = RDDCollection(in._1._1)
      val in1_2 = RDDCollection(in._1._2)
      val in2 = RDDCollection(in._2)
      svDotDV(in1_1, in1_2, in2)
    }

    lazy val ddmvm = fun { p: Rep[(Array[Array[Double]], Array[Double])] =>
      val Pair(m, v) = p
      val matrix: Matrix[Double] = RowMajorDirectMatrix(Collection(m.map { r: Arr[Double] => DenseVector(Collection(r)) }))
      val vector: Vector[Double] = DenseVector(Collection(v))
      (matrix * vector).items.arr
    }

    lazy val dsmvm = fun { p: Rep[(Array[Array[Double]], (Array[Int], (Array[Double], Int)))] =>
      val Tuple(m, vIs, vVs, vL) = p
      val matrix: Matrix[Double] = RowMajorDirectMatrix(Collection(m.map { r: Arr[Double] => DenseVector(Collection(r)) }))
      val vector: Vector[Double] = SparseVector(Collection(vIs), Collection(vVs), vL)
      (matrix * vector).items.arr
    }

    lazy val sdmvm = fun { p: Rep[(Array[(Array[Int], (Array[Double], Int))], Array[Double])] =>
      val Pair(m, v) = p
      val width = m(0)._3
      val matrix: Matrix[Double] = RowMajorSparseMatrix(Collection(m.map {
        r: Rep[(Array[Int], (Array[Double], Int))] =>
          SparseVector(Collection(r._1), Collection(r._2), r._3)}), width)
      val vector: Vector[Double] = DenseVector(Collection(v))
      (matrix * vector).items.arr
    }

    lazy val ssmvm = fun { p: Rep[(Array[(Array[Int], (Array[Double], Int))], (Array[Int], (Array[Double], Int)))] =>
      val Tuple(m, vIs, vVs, vL) = p
      val width = m(0)._3
      val matrix: Matrix[Double] = RowMajorSparseMatrix(Collection(m.map {
        r: Rep[(Array[Int], (Array[Double], Int))] =>
          SparseVector(Collection(r._1), Collection(r._2), r._3) }), width)
      val vector: Vector[Double] = SparseVector(Collection(vIs), Collection(vVs), vL)
      (matrix * vector).items.arr
    }

  }

  test("simpleSparkLAStaged") {
    val ctx = new TestContext(this, "simpleSparkLAStaged") with SimpleLASparkTests with SparkDslExp with RDDCollectionsDslExp with LADslExp {
      val sparkContext = globalSparkContext
      val sSparkContext = ExpSSparkContextImpl(globalSparkContext)
      val repSparkContext = SSparkContext(SSparkConf())
    }

    ctx.emit("dvDotDV", ctx.funDvDotDv)
    ctx.emit("svDotDV", ctx.funSvDotDv)
  }

}


