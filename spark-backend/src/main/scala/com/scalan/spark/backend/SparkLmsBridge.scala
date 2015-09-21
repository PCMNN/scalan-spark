package com.scalan.spark.backend

/**
 * Created by afilippov on 7/14/15.
 */
import scalan.{CommunityMethodMappingDSL, ScalanCommunityDslExp}
import scalan.compilation.lms.CommunityBridge

trait SparkLmsBridge extends CommunityBridge { self: CommunityMethodMappingDSL =>
  import scalan._

  val lms: SparkLmsBackendBase

  override def transformDef[T](m: LmsMirror, g: AstGraph, sym: Exp[T], d: Def[T]) = d match {

    //    case Reflect(NumericRand(bound, i), u, es) =>
    //      reflectMirrored(Reflect(ListForeach(f(a),f(x).asInstanceOf[Sym[A]],f(b)), mapOver(f,u), f(es)))(mtype(manifest[A]), pos)
    // TODO: should be made into effectful primitive
    case NumericRand(bound, i) =>
      val bound_ = m.symMirror[Double](bound)
      val exp = lms.numeric_Random(bound_)
      m.addSym(sym, exp)

    case ArrayRandomGaussian(a, e, xs) =>
      xs.elem match {
        case el: ArrayElem[_] =>
          elemToManifest(el.eItem) match {
            case (mA: Manifest[a]) =>
              val array = m.symMirror[Array[Double]](xs)
              val median = m.symMirror[Double](a)
              val dev = m.symMirror[Double](e)
              val exp = lms.array_randomGaussianSparkLms[a](median, dev, array)(mA)
              m.addSym(sym, exp)
          }
      }
    case _ => super.transformDef(m, g, sym, d)
  }
}
