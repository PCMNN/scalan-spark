package ml

import scalan.ml.{CF_Tests, MLDslSeq}
import scalan.{ScalanCtxSeq, BaseShouldTests}

/**
 * Created by afilippov on 3/27/15.
 */
class CF_TestsSuite extends BaseShouldTests {

  "CF RMSE Measuring" should "be equal to 0.5" in {
    val ctx = new ScalanCtxSeq with MLDslSeq with CF_Tests {}
    val len = 2
    val nArr = Array(Array((0, 0.5)), Array((1, 0.5)))
    val arr = nArr.flatMap(v => v)
    val lens = nArr.map(i => i.length)
    val offs = lens.scanLeft(0)((x, y) => x + y).take(lens.length)
    val in = ctx.Tuple(arr, offs zip lens, len)
    val res = (ctx.rmse(in) * 100.0).toInt.toDouble / 100.0
    res should be(0.5)
  }

  "CF test 2x2 RMSE on Baseline training model" should "be equal to 0.82" in {
    val ctx = new ScalanCtxSeq with MLDslSeq with CF_Tests {}
    import ctx.{delta, gamma1, gamma2, lambda6, lambda7}
    val nItems = 2
    val (arrTrain, segsTrain) = ctx.getNArrayWithSegmentsFromJaggedArray(ctx.jArrTrain2x2)
    val (arrTest, segsTest) = ctx.getNArrayWithSegmentsFromJaggedArray(ctx.jArrTest2x2)
    lazy val width = 20
    lazy val maxIterations = 2
    val paramsPaired = ctx.Tuple(maxIterations, delta, gamma1, gamma2, lambda6, lambda7, width)
    val in = ctx.Tuple(paramsPaired, arrTrain, segsTrain, arrTest, segsTest, nItems)
    val res = (ctx.trainAndTestCF(in) * 100.0).toInt.toDouble / 100.0
    println("RMSE 2x2: " + res)
    res should be <= 0.82
  }

  "CF test 3x5 RMSE on Baseline training model" should "be equal to 1.63" in {
    val ctx = new ScalanCtxSeq with MLDslSeq with CF_Tests {}
    import ctx.{delta, gamma1, gamma2, lambda6, lambda7}
    val nItems = 5
    val (arrTrain, segsTrain) = ctx.getNArrayWithSegmentsFromJaggedArray(ctx.jArrTrain3x5)
    val (arrTest, segsTest) = ctx.getNArrayWithSegmentsFromJaggedArray(ctx.jArrTest3x5)
    lazy val width = 20
    lazy val maxIterations = 2
    val paramsPaired = ctx.Tuple(maxIterations, delta, gamma1, gamma2, lambda6, lambda7, width)
    val in = ctx.Tuple(paramsPaired, arrTrain, segsTrain, arrTest, segsTest, nItems)
    val res = (ctx.trainAndTestCF(in) * 100.0).toInt.toDouble / 100.0
    println("RMSE 3x5: " + res)
    res should be <= 1.63
  }
}