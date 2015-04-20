/**
 * Created by afilippov on 3/30/15.
 */
package la

import java.io.File
import java.lang.reflect.Method

import org.apache.spark._
import org.scalatest.BeforeAndAfterAll

import scala.language.reflectiveCalls
import scalan.la.{LADslExp, LADsl}
import scalan.ml.{MLDslExp, ExampleSVDpp, MLDsl}
import scalan.util.FileUtil
import scalan.{BaseTests, ScalanDsl}
import scalan.spark.collections.{RDDCollectionsDsl, RDDCollectionsDslExp}
import scalan.spark.{SparkDsl, SparkDslExp}
import scalan.compilation.{GraphVizConfig, Passes}

class LATests extends BaseTests with BeforeAndAfterAll { suite =>
  val globalSparkConf = new SparkConf().setAppName("R/W Broadcast").setMaster("local")
  var globalSparkContext: SparkContext = null

  override def beforeAll() = {
    globalSparkContext = new SparkContext(globalSparkConf)
  }

  override def afterAll() = {
    globalSparkContext.stop()
  }

  class Context  extends SimpleLASparkTests with MLDslExp with SparkDslExp with RDDCollectionsDslExp
  with scalan.compilation.DummyCompilerWithPasses {
    val sparkContext = globalSparkContext
    val sSparkContext = ExpSSparkContextImpl(globalSparkContext)
    val repSparkContext = SSparkContext(SSparkConf())

    def emitGraph[A, B](functionName: String, func: Exp[A => B]): Unit = {
      val sourcesDir = FileUtil.file(suite.prefix, functionName)
      val g0 = new PGraph(func)
      val dotFile = new File(sourcesDir, s"$functionName.dot")
      emitDepGraph(g0, dotFile)(defaultGraphVizConfig)

      val passes = Seq(AllUnpackEnabler, AllInvokeEnabler)

      val numPassesLength = passes.length.toString.length

      passes.zipWithIndex.foldLeft(g0) { case (graph, (passFunc, index)) =>
        val pass = passFunc(graph)
        val graph1 = pass(graph).withoutContext

        val indexStr = (index + 1).toString
        val dotFileName = s"${functionName}_${"0" * (numPassesLength - indexStr.length) + indexStr}_${pass.name}.dot"
        emitDepGraph(graph1, new File(sourcesDir, dotFileName))(defaultGraphVizConfig)
        graph1
      }
    }
  }

  test("simpleSparkLAStaged") {
    val ctx1 = new Context
    ctx1.emitGraph("dvDotDV", ctx1.funDvDotDv)

    val ctx2 = new Context
    ctx2.emitGraph("svDotDV", ctx2.funSvDotDv)

    val ctx3 = new Context
    ctx3.emitGraph("ddMVM", ctx3.ddmvm)

    //val ctx4 = new Context
    //ctx4.emitGraph("trainAndTestCF", ctx4.trainAndTestCF)

    val ctx5 = new Context
    ctx5.emitGraph("rmse", ctx5.rmse)

    val ctx6 = new Context
    ctx6.emitGraph("flatMapDomain", ctx6.flatMapDomain)
  }

}


