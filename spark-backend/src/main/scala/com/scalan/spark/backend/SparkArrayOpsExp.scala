package com.scalan.spark.backend

/**
 * Created by afilippov on 7/14/15.
 */
import scala.reflect.SourceContext
import scala.virtualization.lms.common._
import scala.virtualization.lms.internal.Transforming
import scalan.compilation.lms.LmsBackendFacade
import scalan.compilation.lms.cxx.sharedptr.CxxShptrCodegen

trait SparkArrayOpsExp extends Variables with Transforming with VariablesExp { self: LmsBackendFacade =>

  case class ArrayRandomGaussian[A:Manifest](a: Exp[Double], e: Exp[Double], xs: Exp[Array[Double]]) extends Def[Array[Double]] {
    val m = manifest[Double]
  }

  def array_randomGaussian[A:Manifest](a: Rep[Double], e: Rep[Double], arr: Rep[Array[Double]]): Rep[Array[Double]] = ArrayRandomGaussian(a, e, arr)

  override def mirror[A:Manifest](e: Def[A], f: Transformer)(implicit pos: SourceContext): Exp[A] = (e match {
    case ArrayRandomGaussian(a, d, arr) => array_randomGaussian(f(a), f(d), f(arr))(mtype(manifest[Double])).asInstanceOf[Exp[A]]
    case _ => super.mirror(e,f)
  }).asInstanceOf[Exp[A]]
}

trait SparkScalaGenArrayOps extends ScalaGenEffect {
  val IR: SparkArrayOpsExp
  import IR._

  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = rhs match {
    case ds @ ArrayRandomGaussian(a, d, arr) =>
      // TODO use proper source quasiquoter
      stream.println("// generating Random Gaussian values in array")
      stream.println("val " + quote(sym) + " = " + quote(arr) + ".map { _ => util.Random.nextGaussian() * " +
        quote(d) + ".asInstanceOf[Double] + " + quote(a) + ".asInstanceOf[Double] }")
    case _ => super.emitNode(sym, rhs)
  }
}

trait CxxShptrGenArrayOpsLA extends CxxShptrCodegen {
  val IR: SparkArrayOpsExp
  import IR._

  headerFiles ++= Seq("scalan/algorithm.hpp")

  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = rhs match {
    case _ => super.emitNode(sym, rhs)
  }
}
