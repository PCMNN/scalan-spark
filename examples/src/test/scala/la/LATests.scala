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

  trait SimpleLASparkTests extends MLDsl with SparkDsl with RDDCollectionsDsl with ExampleSVDpp {
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

    lazy val ddmvm = fun { p: Rep[(SRDD[Array[Double]], Array[Double])] =>
      val Pair(m, v) = p
      val matrix: Matrix[Double] = RowMajorDirectMatrix(RDDCollection(m.map { r: Arr[Double] => DenseVector(Collection(r)) }))
      val vector: Vector[Double] = DenseVector(Collection(v))
      (matrix * vector).items.arr
    }

    lazy val trainAndTestCF = fun { in: Rep[(ParametersPaired, (Array[(Int, Double)], (Array[(Int, Int)],
      (Array[(Int, Double)], (Array[(Int, Int)],
        (Int, Double))))))] =>
      val Tuple(parametersPaired, arrFlat, segsArr, arrTFlat, segsTArr, nItems, stddev) = in
      val nColl: NColl[(Int, Double)] = NestedCollection(Collection(arrFlat), CollectionOfPairs(segsArr))
      val mR: Dataset1 = RowMajorSparseMatrix.fromNColl(nColl, nItems)
      val nCollT: NColl[(Int, Double)] = NestedCollection(Collection(arrTFlat), CollectionOfPairs(segsTArr))
      val mT: Dataset1 = RowMajorSparseMatrix.fromNColl(nCollT, nItems)
      val params = ParametersSVDpp.init(parametersPaired)
      val data = DatasetCF(mR, mR)
      val closure1 = Tuple(parametersPaired, mR, mR)
      val stateFinal = train(closure1, stddev)
      val rmse = predict(mT, mT, stateFinal.model)
      //printString("Cross-validating RMSE: " + rmse)
      rmse
    }

    lazy val rmse = fun { in: Rep[(SRDD[Array[(Int, Double)]], Int)] =>
      val Tuple(arrs, nItems) = in
      val idxs = arrs.map { r: Arr[(Int,Double)] => r.map{_._1}}
      val vals = arrs.map { r: Arr[(Int,Double)] => r.map{_._2}}

      val rows = (idxs zip vals).map{ r: Rep[(Array[Int], Array[Double])] => SparseVector(Collection(r._1), Collection(r._2), nItems)}
      val matrix: Matrix[Double] = RowMajorSparseMatrix(RDDCollection(rows), nItems)
      calculateRMSE(matrix)
    }

    lazy val flatMapDomain = fun  { in: Rep[SRDD[Array[Array[Double]]]] =>
      val rows = RDDCollection(in).flatMap { r: Rep[Array[Array[Double]]] => Collection(r.map { r1: Rep[Array[Double]] => DenseVector(Collection(r1)) }) }
      val matrix: Matrix[Double] = RowMajorDirectMatrix(rows)
      calculateRMSE(matrix)
    }
  }

  class Context  extends SimpleLASparkTests with MLDslExp with SparkDslExp with RDDCollectionsDslExp
  with scalan.compilation.DummyCompilerWithPasses {
    val sparkContext = globalSparkContext
    val sSparkContext = ExpSSparkContextImpl(globalSparkContext)
    val repSparkContext = SSparkContext(SSparkConf())

    def emitGraph[A, B](functionName: String, func: Exp[A => B]): Unit = {
      val sourcesDir = FileUtil.file(this.prefix, functionName)
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


