package scalan.spark

import java.io.File
import org.apache.spark._
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
  }
}

