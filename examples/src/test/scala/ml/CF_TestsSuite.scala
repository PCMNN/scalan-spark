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

  "CF test 2x2 RMSE on Baseline training model" should "be lower than RMSE on Baseline (~0.826)" in {
    val ctx = new ScalanCtxSeq with MLDslSeq with CF_Tests {}
    import ctx.{delta, gamma1, gamma2, lambda6, lambda7, stepDecrease}
    val nItems = 2
    val (arrTrain, segsTrain) = ctx.getNArrayWithSegmentsFromJaggedArray(ctx.jArrTrain2x2)
    val (arrTest, segsTest) = ctx.getNArrayWithSegmentsFromJaggedArray(ctx.jArrTest2x2)
    lazy val width = 12
    lazy val maxIterations = 12
    lazy val stddev = 0.007
    val paramsPaired = ctx.Tuple(maxIterations, delta, gamma1, gamma2, lambda6, lambda7, width, stepDecrease)
    val in = ctx.Tuple(paramsPaired, arrTrain, segsTrain, arrTest, segsTest, nItems, stddev)
    val res = ctx.trainAndTestCF(in)
    println("[SVD++] RMSE 2x2: " + res)
    val inBL = ctx.Tuple(paramsPaired, arrTrain, segsTrain, arrTest, segsTest, nItems, 0.0)
    val resBL = ctx.trainAndTestCF(inBL)
    println("[BL]    RMSE 2x2: " + resBL)
    res should be <= resBL
  }

  "CF test 3x5 RMSE on Baseline training model" should "be lower than RMSE on Baseline (~1.633)" in {
    val ctx = new ScalanCtxSeq with MLDslSeq with CF_Tests {}
    import ctx.{delta, gamma1, gamma2, lambda6, lambda7, stepDecrease}
    val nItems = 5
    val (arrTrain, segsTrain) = ctx.getNArrayWithSegmentsFromJaggedArray(ctx.jArrTrain3x5)
    val (arrTest, segsTest) = ctx.getNArrayWithSegmentsFromJaggedArray(ctx.jArrTest3x5)
    lazy val width = 12
    lazy val maxIterations = 24
    lazy val stddev = 0.003
    val paramsPaired = ctx.Tuple(maxIterations, delta, gamma1, gamma2, lambda6, lambda7, width, stepDecrease)
    val in = ctx.Tuple(paramsPaired, arrTrain, segsTrain, arrTest, segsTest, nItems, stddev)
    val res = ctx.trainAndTestCF(in)
    println("[SVD++] RMSE 3x5: " + res)
    val inBL = ctx.Tuple(paramsPaired, arrTrain, segsTrain, arrTest, segsTest, nItems, 0.0)
    val resBL = ctx.trainAndTestCF(inBL)
    println("[BL]    RMSE 3x5: " + resBL)
    res should be <= resBL
  }
}