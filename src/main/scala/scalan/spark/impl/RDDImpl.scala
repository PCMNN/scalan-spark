package scalan.spark
package impl

import scalan._
import org.apache.spark.rdd.RDD
import scala.reflect.runtime.universe._
import scalan.common.Default

trait RDDsAbs extends Scalan with RDDs
{ self: RDDsDsl =>
  // single proxy for each type family
  implicit def proxySRDD[A](p: Rep[SRDD[A]]): SRDD[A] =
    proxyOps[SRDD[A]](p)
  // BaseTypeEx proxy
  implicit def proxyRDD[A](p: Rep[RDD[A]]): SRDD[A] =
    proxyOps[SRDD[A]](p.asRep[SRDD[A]])
  implicit def RDDElement[A:Elem]: Elem[RDD[A]] = new BaseElemEx[RDD[A], SRDD[A]](element[SRDD[A]])
  implicit lazy val DefaultOfRDD[A:Elem]: Default[RDD[A]] = SRDD.defaultVal

  abstract class SRDDElem[A, From, To <: SRDD[A]](iso: Iso[From, To]) extends ViewElem[From, To]()(iso)

  trait SRDDCompanionElem extends CompanionElem[SRDDCompanionAbs]
  implicit lazy val SRDDCompanionElem: SRDDCompanionElem = new SRDDCompanionElem {
    lazy val tag = typeTag[SRDDCompanionAbs]
    lazy val defaultRep = Default.defaultVal(SRDD)
  }

  abstract class SRDDCompanionAbs extends CompanionBase[SRDDCompanionAbs] with SRDDCompanion {
    override def toString = "SRDD"
  }
  def SRDD: Rep[SRDDCompanionAbs]
  implicit def proxySRDDCompanion(p: Rep[SRDDCompanion]): SRDDCompanion = {
    proxyOps[SRDDCompanion](p)
  }


}

trait RDDsSeq extends RDDsAbs with RDDsDsl with ScalanSeq {
  lazy val SRDD: Rep[SRDDCompanionAbs] = new SRDDCompanionAbs with UserTypeSeq[SRDDCompanionAbs, SRDDCompanionAbs] {
    lazy val selfType = element[SRDDCompanionAbs]
  }


}

trait RDDsExp extends RDDsAbs with RDDsDsl with ScalanExp {
  lazy val SRDD: Rep[SRDDCompanionAbs] = new SRDDCompanionAbs with UserTypeDef[SRDDCompanionAbs, SRDDCompanionAbs] {
    lazy val selfType = element[SRDDCompanionAbs]
    override def mirror(t: Transformer) = this
  }



  object SRDDMethods {
    object map {
      def unapply(d: Def[_]): Option[(Rep[SRDD[A]], Rep[A => B]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(f, _*)) if receiver.elem.isInstanceOf[SRDDElem[A, _, _] forSome {type A}] && method.getName == "map" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[SRDD[A]], Rep[A => B]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SRDD[A]], Rep[A => B]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object flatMap {
      def unapply(d: Def[_]): Option[(Rep[SRDD[A]], Rep[A => TraversableOnce[B]]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(f, _*)) if receiver.elem.isInstanceOf[SRDDElem[A, _, _] forSome {type A}] && method.getName == "flatMap" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[SRDD[A]], Rep[A => TraversableOnce[B]]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SRDD[A]], Rep[A => TraversableOnce[B]]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object union {
      def unapply(d: Def[_]): Option[(Rep[SRDD[A]], RepRDD[A]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(other, _*)) if receiver.elem.isInstanceOf[SRDDElem[A, _, _] forSome {type A}] && method.getName == "union" =>
          Some((receiver, other)).asInstanceOf[Option[(Rep[SRDD[A]], RepRDD[A]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SRDD[A]], RepRDD[A]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object ++ {
      def unapply(d: Def[_]): Option[(Rep[SRDD[A]], RepRDD[A]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(other, _*)) if receiver.elem.isInstanceOf[SRDDElem[A, _, _] forSome {type A}] && method.getName == "$plus$plus" =>
          Some((receiver, other)).asInstanceOf[Option[(Rep[SRDD[A]], RepRDD[A]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SRDD[A]], RepRDD[A]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object fold {
      def unapply(d: Def[_]): Option[(Rep[SRDD[A]], Rep[A], Rep[((A,A)) => A]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(zeroValue, op, _*)) if receiver.elem.isInstanceOf[SRDDElem[A, _, _] forSome {type A}] && method.getName == "fold" =>
          Some((receiver, zeroValue, op)).asInstanceOf[Option[(Rep[SRDD[A]], Rep[A], Rep[((A,A)) => A]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SRDD[A]], Rep[A], Rep[((A,A)) => A]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object cartesian {
      def unapply(d: Def[_]): Option[(Rep[SRDD[A]], RepRDD[B]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(other, _*)) if receiver.elem.isInstanceOf[SRDDElem[A, _, _] forSome {type A}] && method.getName == "cartesian" =>
          Some((receiver, other)).asInstanceOf[Option[(Rep[SRDD[A]], RepRDD[B]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SRDD[A]], RepRDD[B]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object subtract {
      def unapply(d: Def[_]): Option[(Rep[SRDD[A]], RepRDD[A]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(other, _*)) if receiver.elem.isInstanceOf[SRDDElem[A, _, _] forSome {type A}] && method.getName == "subtract" =>
          Some((receiver, other)).asInstanceOf[Option[(Rep[SRDD[A]], RepRDD[A]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SRDD[A]], RepRDD[A]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }


}
