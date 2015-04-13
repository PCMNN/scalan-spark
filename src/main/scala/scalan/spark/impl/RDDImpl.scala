package scalan.spark
package impl

import scalan._
import org.apache.spark.rdd.RDD
import scalan.common.Default
import scala.reflect._
import scala.reflect.runtime.universe._
import scalan.common.Default

// Abs -----------------------------------
trait RDDsAbs extends ScalanCommunityDsl with RDDs {
  self: SparkDsl =>
  // single proxy for each type family
  implicit def proxySRDD[A](p: Rep[SRDD[A]]): SRDD[A] = {
    proxyOps[SRDD[A]](p)(classTag[SRDD[A]])
  }
  // BaseTypeEx proxy
  //implicit def proxyRDD[A:Elem](p: Rep[RDD[A]]): SRDD[A] =
  //  proxyOps[SRDD[A]](p.asRep[SRDD[A]])

  implicit def unwrapValueOfSRDD[A](w: Rep[SRDD[A]]): Rep[RDD[A]] = w.wrappedValueOfBaseType

  implicit def defaultSRDDElem[A:Elem]: Elem[SRDD[A]] = element[SRDDImpl[A]].asElem[SRDD[A]]
  implicit def rDDElement[A:Elem]: Elem[RDD[A]]

  implicit def castSRDDElement[A](elem: Elem[SRDD[A]]): SRDDElem[A, SRDD[A]] = elem.asInstanceOf[SRDDElem[A, SRDD[A]]]
  implicit val containerSRDD: Cont[SRDD] = new Container[SRDD] {
    def tag[A](implicit evA: WeakTypeTag[A]) = weakTypeTag[SRDD[A]]
    def lift[A](implicit evA: Elem[A]) = element[SRDD[A]]
  }
  case class SRDDIso[A,B](iso: Iso[A,B]) extends Iso1[A, B, SRDD](iso) {
    implicit val eA = iso.eFrom
    implicit val eB = iso.eTo
    def from(x: Rep[SRDD[B]]) = x.map(iso.from _)
    def to(x: Rep[SRDD[A]]) = x.map(iso.to _)
    lazy val defaultRepTo = Default.defaultVal(SRDD.empty[B])
  }
  class SRDDElem[A, To <: SRDD[A]](implicit val eA: Elem[A])
    extends EntityElem1[A, To, SRDD](eA,container[SRDD]) {
    override def isEntityType = true
    override def tag = {
      implicit val tagA = eA.tag
      weakTypeTag[SRDD[A]].asInstanceOf[WeakTypeTag[To]]
    }
    override def convert(x: Rep[Reifiable[_]]) = convertSRDD(x.asRep[SRDD[A]])
    def convertSRDD(x : Rep[SRDD[A]]): Rep[To] = {
      assert(x.selfType1.isInstanceOf[SRDDElem[_,_]])
      x.asRep[To]
    }
    override def getDefaultRep: Rep[To] = ???
  }

  implicit def sRDDElement[A](implicit eA: Elem[A]) =
    new SRDDElem[A, SRDD[A]]()(eA)

  trait SRDDCompanionElem extends CompanionElem[SRDDCompanionAbs]
  implicit lazy val SRDDCompanionElem: SRDDCompanionElem = new SRDDCompanionElem {
    lazy val tag = weakTypeTag[SRDDCompanionAbs]
    protected def getDefaultRep = SRDD
  }

  abstract class SRDDCompanionAbs extends CompanionBase[SRDDCompanionAbs] with SRDDCompanion {
    override def toString = "SRDD"
  }
  def SRDD: Rep[SRDDCompanionAbs]
  implicit def proxySRDDCompanion(p: Rep[SRDDCompanion]): SRDDCompanion = {
    proxyOps[SRDDCompanion](p)
  }

  // default wrapper implementation
  abstract class SRDDImpl[A](val wrappedValueOfBaseType: Rep[RDD[A]])(implicit val eA: Elem[A]) extends SRDD[A] {
    def map[B:Elem](f: Rep[A => B]): Rep[SRDD[B]] =
      methodCallEx[SRDD[B]](self,
        this.getClass.getMethod("map", classOf[AnyRef], classOf[Elem[B]]),
        List(f.asInstanceOf[AnyRef], element[B]))

    def filter(f: Rep[A => Boolean]): Rep[SRDD[A]] =
      methodCallEx[SRDD[A]](self,
        this.getClass.getMethod("filter", classOf[AnyRef]),
        List(f.asInstanceOf[AnyRef]))

    def flatMap[B:Elem](f: Rep[A => SSeq[B]]): Rep[SRDD[B]] =
      methodCallEx[SRDD[B]](self,
        this.getClass.getMethod("flatMap", classOf[AnyRef], classOf[Elem[B]]),
        List(f.asInstanceOf[AnyRef], element[B]))

    def union(other: Rep[SRDD[A]]): Rep[SRDD[A]] =
      methodCallEx[SRDD[A]](self,
        this.getClass.getMethod("union", classOf[AnyRef]),
        List(other.asInstanceOf[AnyRef]))

    def fold(zeroValue: Rep[A])(op: Rep[((A, A)) => A]): Rep[A] =
      methodCallEx[A](self,
        this.getClass.getMethod("fold", classOf[AnyRef], classOf[AnyRef]),
        List(zeroValue.asInstanceOf[AnyRef], op.asInstanceOf[AnyRef]))

    def cartesian[B:Elem](other: Rep[SRDD[B]]): Rep[SRDD[(A, B)]] =
      methodCallEx[SRDD[(A, B)]](self,
        this.getClass.getMethod("cartesian", classOf[AnyRef], classOf[Elem[B]]),
        List(other.asInstanceOf[AnyRef], element[B]))

    def subtract(other: Rep[SRDD[A]]): Rep[SRDD[A]] =
      methodCallEx[SRDD[A]](self,
        this.getClass.getMethod("subtract", classOf[AnyRef]),
        List(other.asInstanceOf[AnyRef]))

    def zip[B:Elem](other: Rep[SRDD[B]]): Rep[SRDD[(A, B)]] =
      methodCallEx[SRDD[(A, B)]](self,
        this.getClass.getMethod("zip", classOf[AnyRef], classOf[Elem[B]]),
        List(other.asInstanceOf[AnyRef], element[B]))

    def zipWithIndex: Rep[SRDD[(A, Long)]] =
      methodCallEx[SRDD[(A, Long)]](self,
        this.getClass.getMethod("zipWithIndex"),
        List())

    def cache: Rep[SRDD[A]] =
      methodCallEx[SRDD[A]](self,
        this.getClass.getMethod("cache"),
        List())

    def first: Rep[A] =
      methodCallEx[A](self,
        this.getClass.getMethod("first"),
        List())

    def count: Rep[Long] =
      methodCallEx[Long](self,
        this.getClass.getMethod("count"),
        List())

    def collect: Rep[Array[A]] =
      methodCallEx[Array[A]](self,
        this.getClass.getMethod("collect"),
        List())
  }
  trait SRDDImplCompanion
  // elem for concrete class
  class SRDDImplElem[A](val iso: Iso[SRDDImplData[A], SRDDImpl[A]])(implicit eA: Elem[A])
    extends SRDDElem[A, SRDDImpl[A]]
    with ViewElem1[A, SRDDImplData[A], SRDDImpl[A], SRDD] {
    override def convertSRDD(x: Rep[SRDD[A]]) = SRDDImpl(x.wrappedValueOfBaseType)
    override def getDefaultRep = super[ViewElem1].getDefaultRep
    override lazy val tag = super[ViewElem1].tag
  }

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
      implicit val tagA = eA.tag
      weakTypeTag[SRDDImpl[A]]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[SRDDImpl[A]]](SRDDImpl(DefaultOfRDD[A].value))
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
    lazy val tag = weakTypeTag[SRDDImplCompanionAbs]
    protected def getDefaultRep = SRDDImpl
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

// Seq -----------------------------------
trait RDDsSeq extends RDDsDsl with ScalanCommunityDslSeq {
  self: SparkDslSeq =>
  lazy val SRDD: Rep[SRDDCompanionAbs] = new SRDDCompanionAbs with UserTypeSeq[SRDDCompanionAbs, SRDDCompanionAbs] {
    lazy val selfType = element[SRDDCompanionAbs]
  }

    // override proxy if we deal with BaseTypeEx
  //override def proxyRDD[A:Elem](p: Rep[RDD[A]]): SRDD[A] =
  //  proxyOpsEx[RDD[A],SRDD[A], SeqSRDDImpl[A]](p, bt => SeqSRDDImpl(bt))

    implicit def rDDElement[A:Elem]: Elem[RDD[A]] = new SeqBaseElemEx[RDD[A], SRDD[A]](element[SRDD[A]])(weakTypeTag[RDD[A]], DefaultOfRDD[A])

  case class SeqSRDDImpl[A]
      (override val wrappedValueOfBaseType: Rep[RDD[A]])
      (implicit eA: Elem[A])
    extends SRDDImpl[A](wrappedValueOfBaseType)
        with UserTypeSeq[SRDD[A], SRDDImpl[A]] {
    lazy val selfType = element[SRDDImpl[A]].asInstanceOf[Elem[SRDD[A]]]
    override def map[B:Elem](f: Rep[A => B]): Rep[SRDD[B]] =
      SRDDImpl(wrappedValueOfBaseType.map[B](f))

    override def filter(f: Rep[A => Boolean]): Rep[SRDD[A]] =
      SRDDImpl(wrappedValueOfBaseType.filter(f))

    override def flatMap[B:Elem](f: Rep[A => SSeq[B]]): Rep[SRDD[B]] =
      SRDDImpl(wrappedValueOfBaseType.flatMap[B](in => f(in).wrappedValueOfBaseType))

    override def union(other: Rep[SRDD[A]]): Rep[SRDD[A]] =
      SRDDImpl(wrappedValueOfBaseType.union(other))

    override def fold(zeroValue: Rep[A])(op: Rep[((A, A)) => A]): Rep[A] =
      wrappedValueOfBaseType.fold(zeroValue)(scala.Function.untupled(op))

    override def cartesian[B:Elem](other: Rep[SRDD[B]]): Rep[SRDD[(A, B)]] =
      SRDDImpl(wrappedValueOfBaseType.cartesian[B](other))

    override def subtract(other: Rep[SRDD[A]]): Rep[SRDD[A]] =
      SRDDImpl(wrappedValueOfBaseType.subtract(other))

    override def zip[B:Elem](other: Rep[SRDD[B]]): Rep[SRDD[(A, B)]] =
      SRDDImpl(wrappedValueOfBaseType.zip[B](other))

    override def zipWithIndex: Rep[SRDD[(A, Long)]] =
      SRDDImpl(wrappedValueOfBaseType.zipWithIndex)

    override def cache: Rep[SRDD[A]] =
      SRDDImpl(wrappedValueOfBaseType.cache)

    override def first: Rep[A] =
      wrappedValueOfBaseType.first

    override def count: Rep[Long] =
      wrappedValueOfBaseType.count

    override def collect: Rep[Array[A]] =
      wrappedValueOfBaseType.collect
  }
  lazy val SRDDImpl = new SRDDImplCompanionAbs with UserTypeSeq[SRDDImplCompanionAbs, SRDDImplCompanionAbs] {
    lazy val selfType = element[SRDDImplCompanionAbs]
  }

  def mkSRDDImpl[A]
      (wrappedValueOfBaseType: Rep[RDD[A]])(implicit eA: Elem[A]): Rep[SRDDImpl[A]] =
      new SeqSRDDImpl[A](wrappedValueOfBaseType)
  def unmkSRDDImpl[A:Elem](p: Rep[SRDDImpl[A]]) =
    Some((p.wrappedValueOfBaseType))

  implicit def wrapRDDToSRDD[A:Elem](v: RDD[A]): SRDD[A] = SRDDImpl(v)
}

// Exp -----------------------------------
trait RDDsExp extends RDDsDsl with ScalanCommunityDslExp {
  self: SparkDslExp =>
  lazy val SRDD: Rep[SRDDCompanionAbs] = new SRDDCompanionAbs with UserTypeDef[SRDDCompanionAbs, SRDDCompanionAbs] {
    lazy val selfType = element[SRDDCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  case class ViewSRDD[A, B](source: Rep[SRDD[A]])(iso: Iso1[A, B, SRDD])
    extends View1[A, B, SRDD](iso) {
    def copy(source: Rep[SRDD[A]]) = ViewSRDD(source)(iso)
    override def toString = s"ViewSRDD[${innerIso.eTo.name}]($source)"
    override def equals(other: Any) = other match {
      case v: ViewSRDD[_, _] => source == v.source && innerIso.eTo == v.innerIso.eTo
      case _ => false
    }
  }
  implicit def rDDElement[A:Elem]: Elem[RDD[A]] = new ExpBaseElemEx[RDD[A], SRDD[A]](element[SRDD[A]])(weakTypeTag[RDD[A]], DefaultOfRDD[A])
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
    (wrappedValueOfBaseType: Rep[RDD[A]])(implicit eA: Elem[A]): Rep[SRDDImpl[A]] =
    new ExpSRDDImpl[A](wrappedValueOfBaseType)
  def unmkSRDDImpl[A:Elem](p: Rep[SRDDImpl[A]]) =
    Some((p.wrappedValueOfBaseType))

  object SRDDMethods {
    object wrappedValueOfBaseType {
      def unapply(d: Def[_]): Option[Rep[SRDD[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SRDDElem[_, _]] && method.getName == "wrappedValueOfBaseType" =>
          Some(receiver).asInstanceOf[Option[Rep[SRDD[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SRDD[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object map {
      def unapply(d: Def[_]): Option[(Rep[SRDD[A]], Rep[A => B]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[SRDDElem[_, _]] && method.getName == "map" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[SRDD[A]], Rep[A => B]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SRDD[A]], Rep[A => B]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object filter {
      def unapply(d: Def[_]): Option[(Rep[SRDD[A]], Rep[A => Boolean]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[SRDDElem[_, _]] && method.getName == "filter" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[SRDD[A]], Rep[A => Boolean]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SRDD[A]], Rep[A => Boolean]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object flatMap {
      def unapply(d: Def[_]): Option[(Rep[SRDD[A]], Rep[A => SSeq[B]]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[SRDDElem[_, _]] && method.getName == "flatMap" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[SRDD[A]], Rep[A => SSeq[B]]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SRDD[A]], Rep[A => SSeq[B]]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object union {
      def unapply(d: Def[_]): Option[(Rep[SRDD[A]], Rep[SRDD[A]]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(other, _*), _) if receiver.elem.isInstanceOf[SRDDElem[_, _]] && method.getName == "union" =>
          Some((receiver, other)).asInstanceOf[Option[(Rep[SRDD[A]], Rep[SRDD[A]]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SRDD[A]], Rep[SRDD[A]]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object ++ {
      def unapply(d: Def[_]): Option[(Rep[SRDD[A]], Rep[SRDD[A]]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(other, _*), _) if receiver.elem.isInstanceOf[SRDDElem[_, _]] && method.getName == "$plus$plus" =>
          Some((receiver, other)).asInstanceOf[Option[(Rep[SRDD[A]], Rep[SRDD[A]]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SRDD[A]], Rep[SRDD[A]]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object fold {
      def unapply(d: Def[_]): Option[(Rep[SRDD[A]], Rep[A], Rep[((A, A)) => A]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(zeroValue, op, _*), _) if receiver.elem.isInstanceOf[SRDDElem[_, _]] && method.getName == "fold" =>
          Some((receiver, zeroValue, op)).asInstanceOf[Option[(Rep[SRDD[A]], Rep[A], Rep[((A, A)) => A]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SRDD[A]], Rep[A], Rep[((A, A)) => A]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object cartesian {
      def unapply(d: Def[_]): Option[(Rep[SRDD[A]], Rep[SRDD[B]]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(other, _*), _) if receiver.elem.isInstanceOf[SRDDElem[_, _]] && method.getName == "cartesian" =>
          Some((receiver, other)).asInstanceOf[Option[(Rep[SRDD[A]], Rep[SRDD[B]]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SRDD[A]], Rep[SRDD[B]]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object subtract {
      def unapply(d: Def[_]): Option[(Rep[SRDD[A]], Rep[SRDD[A]]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(other, _*), _) if receiver.elem.isInstanceOf[SRDDElem[_, _]] && method.getName == "subtract" =>
          Some((receiver, other)).asInstanceOf[Option[(Rep[SRDD[A]], Rep[SRDD[A]]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SRDD[A]], Rep[SRDD[A]]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object zip {
      def unapply(d: Def[_]): Option[(Rep[SRDD[A]], Rep[SRDD[B]]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(other, _*), _) if receiver.elem.isInstanceOf[SRDDElem[_, _]] && method.getName == "zip" =>
          Some((receiver, other)).asInstanceOf[Option[(Rep[SRDD[A]], Rep[SRDD[B]]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SRDD[A]], Rep[SRDD[B]]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object zipWithIndex {
      def unapply(d: Def[_]): Option[Rep[SRDD[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SRDDElem[_, _]] && method.getName == "zipWithIndex" =>
          Some(receiver).asInstanceOf[Option[Rep[SRDD[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SRDD[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object cache {
      def unapply(d: Def[_]): Option[Rep[SRDD[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SRDDElem[_, _]] && method.getName == "cache" =>
          Some(receiver).asInstanceOf[Option[Rep[SRDD[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SRDD[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object first {
      def unapply(d: Def[_]): Option[Rep[SRDD[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SRDDElem[_, _]] && method.getName == "first" =>
          Some(receiver).asInstanceOf[Option[Rep[SRDD[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SRDD[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object count {
      def unapply(d: Def[_]): Option[Rep[SRDD[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SRDDElem[_, _]] && method.getName == "count" =>
          Some(receiver).asInstanceOf[Option[Rep[SRDD[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SRDD[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object collect {
      def unapply(d: Def[_]): Option[Rep[SRDD[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SRDDElem[_, _]] && method.getName == "collect" =>
          Some(receiver).asInstanceOf[Option[Rep[SRDD[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SRDD[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object SRDDCompanionMethods {
    object apply {
      def unapply(d: Def[_]): Option[Rep[Array[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(arr, _*), _) if receiver.elem.isInstanceOf[SRDDCompanionElem] && method.getName == "apply" =>
          Some(arr).asInstanceOf[Option[Rep[Array[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[Array[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object fromArray {
      def unapply(d: Def[_]): Option[Rep[Array[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(arr, _*), _) if receiver.elem.isInstanceOf[SRDDCompanionElem] && method.getName == "fromArray" =>
          Some(arr).asInstanceOf[Option[Rep[Array[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[Array[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object empty {
      def unapply(d: Def[_]): Option[Unit forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SRDDCompanionElem] && method.getName == "empty" =>
          Some(()).asInstanceOf[Option[Unit forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Unit forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object UserTypeSRDD {
    def unapply(s: Exp[_]): Option[Iso[_, _]] = {
      s.elem match {
        case e: SRDDElem[a,to] => e.eItem match {
          case UnpackableElem(iso) => Some(iso)
          case _ => None
        }
        case _ => None
      }
    }
  }

  override def unapplyViews[T](s: Exp[T]): Option[Unpacked[T]] = (s match {
    case Def(view: ViewSRDD[_, _]) =>
      Some((view.source, view.iso))
    case UserTypeSRDD(iso: Iso[a, b]) =>
      val newIso = SRDDIso(iso)
      val repr = reifyObject(UnpackView(s.asRep[SRDD[b]])(newIso))
      Some((repr, newIso))
    case _ =>
      super.unapplyViews(s)
  }).asInstanceOf[Option[Unpacked[T]]]

  type SRDDFlatMapArgs[A,B] = (Rep[SRDD[A]], Rep[A => SSeq[B]])

  override def rewriteDef[T](d: Def[T]) = d match {
    case SRDDMethods.map(xs, Def(l: Lambda[_, _])) if l.isIdentity => xs
    case SRDDMethods.map(xs, f) => (xs, f) match {
      case (xs: RepRDD[a] @unchecked, f @ Def(Lambda(_, _, _, UnpackableExp(_, iso: Iso[b, c])))) =>
        val f1 = f.asRep[a => c]
        implicit val eA = xs.elem.eItem
        implicit val eB = iso.eFrom
        val s = xs.map(fun { x =>
          val tmp = f1(x)
          iso.from(tmp)
        })
        val res = ViewSRDD(s)(SRDDIso(iso))
        res
      case (Def(view: ViewSRDD[a, b]), f: Rep[Function1[_, c] @unchecked]) =>
        val iso = view.innerIso
        val f1 = f.asRep[b => c]
        implicit val eA = iso.eFrom
        implicit val eB = iso.eTo
        implicit val eC = f1.elem.eRange
        view.source.map(fun { x => f1(iso.to(x)) })
      case _ =>
        super.rewriteDef(d)
    }
    case SRDDMethods.flatMap(t: SRDDFlatMapArgs[_,c] @unchecked) => t match {
      case (xs: RepRDD[a]@unchecked, f @ Def(Lambda(_, _, _, UnpackableExp(_, iso: SSeqIso[b, c])) ) ) => {
        val f1 = f.asRep[a => SSeq[c]]
        val baseIso = iso.iso
        implicit val eA = xs.elem.eItem
        implicit val eB = baseIso.eFrom
        implicit val eC = iso.eFrom

        val s = xs.flatMap( fun { x =>
          val tmp = f1(x)
          iso.from(tmp)
        })
        ViewSRDD(s)(SRDDIso(baseIso))
      }
      case (Def(view: ViewSRDD[a, b]), _) => {
        val iso = view.innerIso
        val ff = t._2.asRep[b => SSeq[c]]
        implicit val eA = iso.eFrom
        implicit val eB = iso.eTo
        implicit val eC =  (ff.elem.eRange match {
          case sEl: SSeqElem[_,_] => sEl.eA
          case _ => !!!
        }).asElem[c]
        view.source.flatMap(fun { x => ff(iso.to(x))})
      }
      case _ =>
        super.rewriteDef(d)
    }

    case view1@ViewSRDD(Def(view2@ViewSRDD(arr))) =>
      val compIso = composeIso(view1.innerIso, view2.innerIso)
      implicit val eAB = compIso.eTo
      ViewSRDD(arr)(SRDDIso(compIso))

    case _ => super.rewriteDef(d)
  }
}
