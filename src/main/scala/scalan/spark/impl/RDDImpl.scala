package scalan.spark
package impl

import scala.reflect.ClassTag
import scalan._
import org.apache.spark.rdd.RDD
import scalan.common.Default
import scala.reflect.runtime.universe._
import scalan.common.Default

trait RDDsAbs extends Scalan with RDDs
{ self: SparkDsl =>
  // single proxy for each type family
  implicit def proxySRDD[A](p: Rep[SRDD[A]]): SRDD[A] =
    proxyOps[SRDD[A]](p)
  // BaseTypeEx proxy
  implicit def proxyRDD[A:Elem](p: Rep[RDD[A]]): SRDD[A] =
    proxyOps[SRDD[A]](p.asRep[SRDD[A]])

  implicit def defaultSRDDElem[A:Elem]: Elem[SRDD[A]] = element[SRDDImpl[A]].asElem[SRDD[A]]
  implicit def RDDElement[A:Elem:WeakTypeTag]: Elem[RDD[A]]

  abstract class SRDDElem[A, From, To <: SRDD[A]](iso: Iso[From, To]) extends ViewElem[From, To]()(iso)

  trait SRDDCompanionElem extends CompanionElem[SRDDCompanionAbs]
  implicit lazy val SRDDCompanionElem: SRDDCompanionElem = new SRDDCompanionElem {
    lazy val tag = typeTag[SRDDCompanionAbs]
    lazy val getDefaultRep = Default.defaultVal(SRDD)
    //def getDefaultRep = defaultRep
  }

  abstract class SRDDCompanionAbs extends CompanionBase[SRDDCompanionAbs] with SRDDCompanion {
    override def toString = "SRDD"
    
  }
  def SRDD: Rep[SRDDCompanionAbs]
  implicit def proxySRDDCompanion(p: Rep[SRDDCompanion]): SRDDCompanion = {
    proxyOps[SRDDCompanion](p)
  }

  //default wrapper implementation
    abstract class SRDDImpl[A](val wrappedValueOfBaseType: Rep[RDD[A]])(implicit val eA: Elem[A]) extends SRDD[A] {
    
    def map[B:Elem](f: Rep[A => B]): Rep[RDD[B]] =
      methodCallEx[RDD[B]](self,
        this.getClass.getMethod("map", classOf[AnyRef], classOf[Elem[B]]),
        List(f.asInstanceOf[AnyRef], element[B]))

    
    def flatMap[B:Elem](f: Rep[A => TraversableOnce[B]]): Rep[RDD[B]] =
      methodCallEx[RDD[B]](self,
        this.getClass.getMethod("flatMap", classOf[AnyRef], classOf[Elem[B]]),
        List(f.asInstanceOf[AnyRef], element[B]))

    
    def union(other: Rep[RDD[A]]): Rep[RDD[A]] =
      methodCallEx[RDD[A]](self,
        this.getClass.getMethod("union", classOf[AnyRef]),
        List(other.asInstanceOf[AnyRef]))

    
    def fold(zeroValue: Rep[A])(op: Rep[((A,A)) => A]): Rep[A] =
      methodCallEx[A](self,
        this.getClass.getMethod("fold", classOf[AnyRef], classOf[AnyRef]),
        List(zeroValue.asInstanceOf[AnyRef], op.asInstanceOf[AnyRef]))

    
    def cartesian[B:Elem](other: Rep[RDD[B]]): Rep[RDD[(A,B)]] =
      methodCallEx[RDD[(A,B)]](self,
        this.getClass.getMethod("cartesian", classOf[AnyRef], classOf[Elem[B]]),
        List(other.asInstanceOf[AnyRef], element[B]))

    
    def subtract(other: Rep[RDD[A]]): Rep[RDD[A]] =
      methodCallEx[RDD[A]](self,
        this.getClass.getMethod("subtract", classOf[AnyRef]),
        List(other.asInstanceOf[AnyRef]))

  }
  trait SRDDImplCompanion
  // elem for concrete class
  class SRDDImplElem[A](iso: Iso[SRDDImplData[A], SRDDImpl[A]]) extends SRDDElem[A, SRDDImplData[A], SRDDImpl[A]](iso)

  // state representation type
  type SRDDImplData[A] = RDD[A]

  // 3) Iso for concrete class
  class SRDDImplIso[A](implicit eA: Elem[A])
    extends Iso[SRDDImplData[A], SRDDImpl[A]] {
    override def from(p: Rep[SRDDImpl[A]]) =
      unmkSRDDImpl(p) match {
        case Some((wrappedValueOfBaseType)) => wrappedValueOfBaseType
        case None => !!!
      }
    override def to(p: Rep[RDD[A]]) = {
      val wrappedValueOfBaseType = p
      SRDDImpl(wrappedValueOfBaseType)
    }
    lazy val tag = {
      weakTypeTag[SRDDImpl[A]]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[SRDDImpl[A]]](SRDDImpl(Default.defaultOf[RDD[A]]))
    lazy val eTo = new SRDDImplElem[A](this)
  }
  // 4) constructor and deconstructor
  abstract class SRDDImplCompanionAbs extends CompanionBase[SRDDImplCompanionAbs] with SRDDImplCompanion {
    override def toString = "SRDDImpl"

    def apply[A](wrappedValueOfBaseType: Rep[RDD[A]])(implicit eA: Elem[A]): Rep[SRDDImpl[A]] =
      mkSRDDImpl(wrappedValueOfBaseType)
    def unapply[A:Elem](p: Rep[SRDDImpl[A]]) = unmkSRDDImpl(p)
  }
  def SRDDImpl: Rep[SRDDImplCompanionAbs]
  implicit def proxySRDDImplCompanion(p: Rep[SRDDImplCompanionAbs]): SRDDImplCompanionAbs = {
    proxyOps[SRDDImplCompanionAbs](p)
  }

  class SRDDImplCompanionElem extends CompanionElem[SRDDImplCompanionAbs] {
    lazy val tag = typeTag[SRDDImplCompanionAbs]
    lazy val getDefaultRep = Default.defaultVal(SRDDImpl)
    //def getDefaultRep = defaultRep
  }
  implicit lazy val SRDDImplCompanionElem: SRDDImplCompanionElem = new SRDDImplCompanionElem

  implicit def proxySRDDImpl[A](p: Rep[SRDDImpl[A]]): SRDDImpl[A] =
    proxyOps[SRDDImpl[A]](p)

  implicit class ExtendedSRDDImpl[A](p: Rep[SRDDImpl[A]])(implicit eA: Elem[A]) {
    def toData: Rep[SRDDImplData[A]] = isoSRDDImpl(eA).from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoSRDDImpl[A](implicit eA: Elem[A]): Iso[SRDDImplData[A], SRDDImpl[A]] =
    new SRDDImplIso[A]

  // 6) smart constructor and deconstructor
  def mkSRDDImpl[A](wrappedValueOfBaseType: Rep[RDD[A]])(implicit eA: Elem[A]): Rep[SRDDImpl[A]]
  def unmkSRDDImpl[A:Elem](p: Rep[SRDDImpl[A]]): Option[(Rep[RDD[A]])]
}

trait RDDsSeq extends RDDsAbs with RDDsDsl with ScalanSeq
{ self: SparkDslSeq =>
  lazy val SRDD: Rep[SRDDCompanionAbs] = new SRDDCompanionAbs with UserTypeSeq[SRDDCompanionAbs, SRDDCompanionAbs] {
    lazy val selfType = element[SRDDCompanionAbs]
    
  }

    // override proxy if we deal with BaseTypeEx
  override def proxyRDD[A:Elem](p: Rep[RDD[A]]): SRDD[A] =
    proxyOpsEx[RDD[A],SRDD[A], SeqSRDDImpl[A]](p, bt => SeqSRDDImpl(bt))

    implicit def RDDElement[A:Elem:WeakTypeTag]: Elem[RDD[A]] = new SeqBaseElemEx[RDD[A], SRDD[A]](element[SRDD[A]])

  case class SeqSRDDImpl[A]
      (override val wrappedValueOfBaseType: Rep[RDD[A]])
      (implicit eA: Elem[A])
    extends SRDDImpl[A](wrappedValueOfBaseType)
        with UserTypeSeq[SRDD[A], SRDDImpl[A]] {
    lazy val selfType = element[SRDDImpl[A]].asInstanceOf[Elem[SRDD[A]]]
    
    override def map[B:Elem](f: Rep[A => B]): Rep[RDD[B]] =
      wrappedValueOfBaseType.map[B](f)

    
    override def flatMap[B:Elem](f: Rep[A => TraversableOnce[B]]): Rep[RDD[B]] =
      wrappedValueOfBaseType.flatMap[B](f)

    
    override def union(other: Rep[RDD[A]]): Rep[RDD[A]] =
      wrappedValueOfBaseType.union(other)

    
    override def $plus$plus(other: Rep[RDD[A]]): Rep[RDD[A]] =
      wrappedValueOfBaseType.$plus$plus(other)

    
    override def fold(zeroValue: Rep[A])(op: Rep[((A,A)) => A]): Rep[A] =
      wrappedValueOfBaseType.fold(zeroValue)((a1, a2) => op(a1, a2))

    
    override def cartesian[B:Elem](other: Rep[RDD[B]]): Rep[RDD[(A,B)]] =
      wrappedValueOfBaseType.cartesian[B](other)

    
    override def subtract(other: Rep[RDD[A]]): Rep[RDD[A]] =
      wrappedValueOfBaseType.subtract(other)

  }
  lazy val SRDDImpl = new SRDDImplCompanionAbs with UserTypeSeq[SRDDImplCompanionAbs, SRDDImplCompanionAbs] {
    lazy val selfType = element[SRDDImplCompanionAbs]
  }

  def mkSRDDImpl[A]
      (wrappedValueOfBaseType: Rep[RDD[A]])(implicit eA: Elem[A]) =
      new SeqSRDDImpl[A](wrappedValueOfBaseType)
  def unmkSRDDImpl[A:Elem](p: Rep[SRDDImpl[A]]) =
    Some((p.wrappedValueOfBaseType))
}

trait RDDsExp extends RDDsAbs with RDDsDsl with ScalanExp
{ self: SparkDslExp =>
  lazy val SRDD: Rep[SRDDCompanionAbs] = new SRDDCompanionAbs with UserTypeDef[SRDDCompanionAbs, SRDDCompanionAbs] {
    lazy val selfType = element[SRDDCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  implicit def RDDElement[A:Elem:WeakTypeTag]: Elem[RDD[A]] = new ExpBaseElemEx[RDD[A], SRDD[A]](element[SRDD[A]])

  case class ExpSRDDImpl[A]
      (override val wrappedValueOfBaseType: Rep[RDD[A]])
      (implicit eA: Elem[A])
    extends SRDDImpl[A](wrappedValueOfBaseType) with UserTypeDef[SRDD[A], SRDDImpl[A]] {
    lazy val selfType = element[SRDDImpl[A]].asInstanceOf[Elem[SRDD[A]]]
    override def mirror(t: Transformer) = ExpSRDDImpl[A](t(wrappedValueOfBaseType))
  }

  lazy val SRDDImpl: Rep[SRDDImplCompanionAbs] = new SRDDImplCompanionAbs with UserTypeDef[SRDDImplCompanionAbs, SRDDImplCompanionAbs] {
    lazy val selfType = element[SRDDImplCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object SRDDImplMethods {

  }



  def mkSRDDImpl[A]
    (wrappedValueOfBaseType: Rep[RDD[A]])(implicit eA: Elem[A]) =
    new ExpSRDDImpl[A](wrappedValueOfBaseType)
  def unmkSRDDImpl[A:Elem](p: Rep[SRDDImpl[A]]) =
    Some((p.wrappedValueOfBaseType))

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
      def unapply(d: Def[_]): Option[(Rep[SRDD[A]], Rep[RDD[A]]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(other, _*)) if receiver.elem.isInstanceOf[SRDDElem[A, _, _] forSome {type A}] && method.getName == "union" =>
          Some((receiver, other)).asInstanceOf[Option[(Rep[SRDD[A]], Rep[RDD[A]]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SRDD[A]], Rep[RDD[A]]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object ++ {
      def unapply(d: Def[_]): Option[(Rep[SRDD[A]], Rep[RDD[A]]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(other, _*)) if receiver.elem.isInstanceOf[SRDDElem[A, _, _] forSome {type A}] && method.getName == "$plus$plus" =>
          Some((receiver, other)).asInstanceOf[Option[(Rep[SRDD[A]], Rep[RDD[A]]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SRDD[A]], Rep[RDD[A]]) forSome {type A}] = exp match {
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
      def unapply(d: Def[_]): Option[(Rep[SRDD[A]], Rep[RDD[B]]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(other, _*)) if receiver.elem.isInstanceOf[SRDDElem[A, _, _] forSome {type A}] && method.getName == "cartesian" =>
          Some((receiver, other)).asInstanceOf[Option[(Rep[SRDD[A]], Rep[RDD[B]]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SRDD[A]], Rep[RDD[B]]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object subtract {
      def unapply(d: Def[_]): Option[(Rep[SRDD[A]], Rep[RDD[A]]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(other, _*)) if receiver.elem.isInstanceOf[SRDDElem[A, _, _] forSome {type A}] && method.getName == "subtract" =>
          Some((receiver, other)).asInstanceOf[Option[(Rep[SRDD[A]], Rep[RDD[A]]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SRDD[A]], Rep[RDD[A]]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object SRDDCompanionMethods {

  }
}
