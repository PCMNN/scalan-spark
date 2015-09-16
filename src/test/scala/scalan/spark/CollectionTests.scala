package scalan.spark

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.scalatest.BeforeAndAfterAll
import scalan._
import la.{SparkLADsl, SparkLADslExp}

class CollectionTests extends BaseTests with BeforeAndAfterAll with TestContexts { suite =>
  val globalSparkConf = new SparkConf().setAppName("Collection Tests").setMaster("local[4]")
  var globalSparkContext: SparkContext = null

  trait CollectionSamples extends ScalanDsl with SparkLADsl {
    val prefix = suite.prefix
    val subfolder = "collection"
    lazy val zipRddVector = fun { rdd: Rep[RDD[(Long, Array[Int])]] =>
      val res = RDDIndexedCollection(SRDDImpl(rdd))
      res.map(rdd => rdd.map { p: Rep[Int] => p + 1 }).arr
    }
  }

  test("zipRddVectorSparkStaged") {
    val ctx = new TestContext("zipRddVectorSparkStaged") with CollectionSamples with SparkLADslExp {
      val sparkContext = globalSparkContext
      val sSparkContext = ExpSSparkContextImpl(globalSparkContext)
      val repSparkContext = SSparkContext(SSparkConf())
    }
    ctx.emit("zipRddVector", ctx.zipRddVector)
  }
}
