package la

/**
 * Created by afilippov on 3/27/15.
 */
import scalan.la.{TestExamplesLA, LADslSeq}
import scalan.{ScalanCtxSeq, BaseShouldTests}

class LA_TestsSuite extends BaseShouldTests {

  "LA_DvDotDv Measuring" should "be equal to 8" in {
    val ctx = new ScalanCtxSeq with LADslSeq with TestExamplesLA {}
    val in1 = Array(1.0, 2.0)
    val in2 = Array(2.0, 3.0)
    val in = Pair(in1, in2)
    val out = Array(2.0, 6.0).reduce(_ + _)
    val res = ctx.funDvDotDv(in)
    res should be(out)
  }
  "LA_DvDotSv Measuring" should "be equal to 8" in {
    val ctx = new ScalanCtxSeq with LADslSeq with TestExamplesLA {}
    val in1 = Array(1.0, 2.0)
    val in2 = Array((0, 2.0), (1, 3.0))
    val len = 2
    val in = ctx.Tuple(in1, in2, len)
    val out = Array(2.0, 6.0).reduce(_ + _)
    val res = ctx.funDvDotSv(in)
    res should be(out)
  }
  "LA_SvDotDv Measuring" should "be equal to 8" in {
    val ctx = new ScalanCtxSeq with LADslSeq with TestExamplesLA {}
    val in1 = Array((0, 1.0), (1, 2.0))
    val in2 = Array(2.0, 3.0)
    val len = 2
    val in = ctx.Tuple(in1, in2, len)
    val out = Array(2.0, 6.0).reduce(_ + _)
    val res = ctx.funSvDotDv(in)
    res should be(out)
  }
  "LA_SvDotSv Measuring" should "be equal to 8" in {
    val ctx = new ScalanCtxSeq with LADslSeq with TestExamplesLA {}
    val in1 = Array((0, 1.0), (1, 2.0))
    val in2 = Array((0, 2.0), (1, 3.0))
    val len = 2
    val in = ctx.Tuple(in1, in2, len)
    val out = Array(2.0, 6.0).reduce(_ + _)
    val res = ctx.funSvDotSv(in)
    res should be(out)
  }

}
