package scalan.spark
package impl

import scalan._
import scalan.spark.{RDD, Broadcast}
import org.apache.spark.SparkContext
import scala.reflect.runtime.universe._
import scalan.common.Default

trait SparkContextsAbs extends Scalan with SparkContexts
{ self: SparkContextsDsl =>
  // single proxy for each type family
  implicit def proxySSparkContext(p: Rep[SSparkContext]): SSparkContext =
    proxyOps[SSparkContext](p)
  // BaseTypeEx proxy
  implicit def proxySparkContext(p: Rep[SparkContext]): SSparkContext =
    proxyOps[SSparkContext](p.asRep[SSparkContext])
  implicit lazy val SparkContextElement: Elem[SparkContext] = new BaseElemEx[SparkContext, SSparkContext](element[SSparkContext])
  implicit lazy val DefaultOfSparkContext: Default[SparkContext] = SSparkContext.defaultVal

  abstract class SSparkContextElem[From, To <: SSparkContext](iso: Iso[From, To]) extends ViewElem[From, To]()(iso)

  trait SSparkContextCompanionElem extends CompanionElem[SSparkContextCompanionAbs]
  implicit lazy val SSparkContextCompanionElem: SSparkContextCompanionElem = new SSparkContextCompanionElem {
    lazy val tag = typeTag[SSparkContextCompanionAbs]
    lazy val defaultRep = Default.defaultVal(SSparkContext)
  }

  abstract class SSparkContextCompanionAbs extends CompanionBase[SSparkContextCompanionAbs] with SSparkContextCompanion {
    override def toString = "SSparkContext"
  }
  def SSparkContext: Rep[SSparkContextCompanionAbs]
  implicit def proxySSparkContextCompanion(p: Rep[SSparkContextCompanion]): SSparkContextCompanion = {
    proxyOps[SSparkContextCompanion](p)
  }


}

trait SparkContextsSeq extends SparkContextsAbs with SparkContextsDsl with ScalanSeq {
  lazy val SSparkContext: Rep[SSparkContextCompanionAbs] = new SSparkContextCompanionAbs with UserTypeSeq[SSparkContextCompanionAbs, SSparkContextCompanionAbs] {
    lazy val selfType = element[SSparkContextCompanionAbs]
  }


}

trait SparkContextsExp extends SparkContextsAbs with SparkContextsDsl with ScalanExp {
  lazy val SSparkContext: Rep[SSparkContextCompanionAbs] = new SSparkContextCompanionAbs with UserTypeDef[SSparkContextCompanionAbs, SSparkContextCompanionAbs] {
    lazy val selfType = element[SSparkContextCompanionAbs]
    override def mirror(t: Transformer) = this
  }



  object SSparkContextMethods {
    object defaultParallelism {
      def unapply(d: Def[_]): Option[Rep[SSparkContext]] = d match {
        case MethodCall(receiver, method, _) if receiver.elem.isInstanceOf[SSparkContextElem[_, _]] && method.getName == "defaultParallelism" =>
          Some(receiver).asInstanceOf[Option[Rep[SSparkContext]]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SSparkContext]] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object broadcast {
      def unapply(d: Def[_]): Option[(Rep[SSparkContext], Rep[T]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(value, _*)) if receiver.elem.isInstanceOf[SSparkContextElem[_, _]] && method.getName == "broadcast" =>
          Some((receiver, value)).asInstanceOf[Option[(Rep[SSparkContext], Rep[T]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SSparkContext], Rep[T]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object makeRDD {
      def unapply(d: Def[_]): Option[(Rep[SSparkContext], Rep[Seq[T]], Rep[Int]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(seq, numSlices, _*)) if receiver.elem.isInstanceOf[SSparkContextElem[_, _]] && method.getName == "makeRDD" =>
          Some((receiver, seq, numSlices)).asInstanceOf[Option[(Rep[SSparkContext], Rep[Seq[T]], Rep[Int]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SSparkContext], Rep[Seq[T]], Rep[Int]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object emptyRDD {
      def unapply(d: Def[_]): Option[Rep[SSparkContext] forSome {type T}] = d match {
        case MethodCall(receiver, method, _) if receiver.elem.isInstanceOf[SSparkContextElem[_, _]] && method.getName == "emptyRDD" =>
          Some(receiver).asInstanceOf[Option[Rep[SSparkContext] forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SSparkContext] forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }


}
