package scalan.examples

import java.io.File
import org.apache.spark.{SparkContext, SparkConf}

import scalan._
import scalan.spark._

class ReadBroadcastTests extends BaseTests { suite =>
  val prefix = new File("test-out/scalan/spark/")
  trait ReadBroadcastSimple extends ScalanDsl with SparkDsl with ReadBroadcast {
    val prefix = suite.prefix
    val subfolder = "simple"
  }

  test("readBroadcastStaged") {
    val ctx = new TestContext with ReadBroadcastSimple with SparkDslExp {
      def print(s: Rep[String]): Rep[Unit] = print(s)
      def read: Rep[String] = Console.readLine()
      val sparkConf = new SparkConf().setAppName("R/W Broadcast").setMaster("local")
      val sparkContext = new SparkContext(sparkConf)
    }

    ctx.emit("main", ctx.main)
  }
}
