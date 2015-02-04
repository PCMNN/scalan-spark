package scalan.spark

import java.lang.reflect.Method

import org.scalatest.words.ResultOfStringPassedToVerb
import org.scalatest.{FlatSpec, FunSuite, Matchers}

import scalan._
import scalan.util.FileUtil

trait TestsUtil {
  def testOutDir = "test-out"

  def testSuffixes = Seq("Suite", "Tests", "It", "_")

  lazy val prefix = {
    val suiteName = testSuffixes.foldLeft(getClass.getName)(_.stripSuffix(_))
    val pathComponents = suiteName.split('.')
    FileUtil.file(testOutDir, pathComponents: _*)
  }
}

// TODO switch to FunSpec and eliminate duplication in test names (e.g. RewriteSuite)
abstract class BaseTests extends FunSuite with Matchers with TestsUtil

abstract class BaseShouldTests extends FlatSpec with Matchers with TestsUtil {
  protected final class InAndIgnoreMethods2(resultOfStringPassedToVerb: ResultOfStringPassedToVerb) {

    import resultOfStringPassedToVerb.rest
    val _inner = new InAndIgnoreMethods(resultOfStringPassedToVerb)
    /**
     * Supports the registration of tests in shorthand form.
     *
     * <p>
     * This method supports syntax such as the following:
     * </p>
     *
     * <pre class="stHighlight">
     * "A Stack" must "pop values in last-in-first-out order" in { ... }
     *                                                        ^
     * </pre>
     *
     * <p>
     * For examples of test registration, see the <a href="FlatSpec.html">main documentation</a>
     * for trait <code>FlatSpec</code>.
     * </p>
     */
    def beArgFor(testFun: String => Unit) {
      _inner.in(testFun(rest.trim))
    }
  }

  protected implicit def convertToInAndIgnoreMethods2(resultOfStringPassedToVerb: ResultOfStringPassedToVerb) =
    new InAndIgnoreMethods2(resultOfStringPassedToVerb)

}

// TODO get current test name programmatically
// see http://stackoverflow.com/questions/14831246/access-scalatest-test-name-from-inside-test
/**
 * Created by slesarenko on 18/01/15.
 */
abstract class TestContext(suite: TestsUtil, testName: String) extends ScalanCtxExp {
  override def isInvokeEnabled(d: Def[_], m: Method) = true
  override def shouldUnpack(e: ViewElem[_, _]) = true
  def emit(name: String, ss: Exp[_]*) =
    emitDepGraph(ss, FileUtil.file(suite.prefix, testName, s"$name.dot"))
}
