package scalan.spark
package impl

import scalan._
import org.apache.spark.rdd.RDD
import scalan.common.Default
import scala.reflect._
import scala.reflect.runtime.universe._

// Abs -----------------------------------
trait RDDsAbs extends RDDs with ScalanCommunityDsl {
  self: SparkDsl =>

  // single proxy for each type family
  implicit def proxySRDD[A](p: Rep[SRDD[A]]): SRDD[A] = {
    proxyOps[SRDD[A]](p)(scala.reflect.classTag[SRDD[A]])
  }

  // TypeWrapper proxy
  //implicit def proxyRDD[A:Elem](p: Rep[RDD[A]]): SRDD[A] =
  //  proxyOps[SRDD[A]](p.asRep[SRDD[A]])

  implicit def unwrapValueOfSRDD[A](w: Rep[SRDD[A]]): Rep[RDD[A]] = w.wrappedValueOfBaseType

  implicit def rDDElement[A:Elem]: Elem[RDD[A]]

  implicit def castSRDDElement[A](elem: Elem[SRDD[A]]): SRDDElem[A, SRDD[A]] = elem.asInstanceOf[SRDDElem[A, SRDD[A]]]

  implicit val containerRDD: Cont[RDD] = new Container[RDD] {
    def tag[A](implicit evA: WeakTypeTag[A]) = weakTypeTag[RDD[A]]
    def lift[A](implicit evA: Elem[A]) = element[RDD[A]]
  }

  implicit val containerSRDD: Cont[SRDD] = new Container[SRDD] {
    def tag[A](implicit evA: WeakTypeTag[A]) = weakTypeTag[SRDD[A]]
    def lift[A](implicit evA: Elem[A]) = element[SRDD[A]]
  }
  case class SRDDIso[A,B](iso: Iso[A,B]) extends Iso1[A, B, SRDD](iso) {
    implicit val eA = iso.eFrom
    implicit val eB = iso.eTo
    def from(x: Rep[SRDD[B]]) = x.map(iso.from _)
    def to(x: Rep[SRDD[A]]) = x.map(iso.to _)
    lazy val defaultRepTo = SRDD.empty[B]
  }

  // familyElem
  abstract class SRDDElem[A, To <: SRDD[A]](implicit val eA: Elem[A])
    extends WrapperElem1[A, To, RDD, SRDD]()(eA, container[RDD], container[SRDD]) {
    override def isEntityType = true
    override lazy val tag = {
      implicit val tagA = eA.tag
      weakTypeTag[SRDD[A]].asInstanceOf[WeakTypeTag[To]]
    }
    override def convert(x: Rep[Reifiable[_]]) = {
      implicit val eTo: Elem[To] = this
      val conv = fun {x: Rep[SRDD[A]] => convertSRDD(x) }
      tryConvert(element[SRDD[A]], this, x, conv)
    }

    def convertSRDD(x : Rep[SRDD[A]]): Rep[To] = {
      assert(x.selfType1 match { case _: SRDDElem[_, _] => true; case _ => false })
      x.asRep[To]
    }
    override def getDefaultRep: Rep[To] = ???
  }

  implicit def sRDDElement[A](implicit eA: Elem[A]): Elem[SRDD[A]] =
    new SRDDElem[A, SRDD[A]] {
      lazy val eTo = element[SRDDImpl[A]]
    }

  implicit case object SRDDCompanionElem extends CompanionElem[SRDDCompanionAbs] {
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

    def zipSafe[B:Elem](other: Rep[SRDD[B]]): Rep[SRDD[(A, B)]] =
      methodCallEx[SRDD[(A, B)]](self,
        this.getClass.getMethod("zipSafe", classOf[AnyRef], classOf[Elem[B]]),
        List(other.asInstanceOf[AnyRef], element[B]))

    def zipWithIndex: Rep[SRDD[(A, Long)]] =
      methodCallEx[SRDD[(A, Long)]](self,
        this.getClass.getMethod("zipWithIndex"),
        List())

    def cache: Rep[SRDD[A]] =
      methodCallEx[SRDD[A]](self,
        this.getClass.getMethod("cache"),
        List())

    def unpersist(blocking: Rep[Boolean]): Rep[SRDD[A]] =
      methodCallEx[SRDD[A]](self,
        this.getClass.getMethod("unpersist", classOf[AnyRef]),
        List(blocking.asInstanceOf[AnyRef]))

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

    def context: Rep[SSparkContext] =
      methodCallEx[SSparkContext](self,
        this.getClass.getMethod("context"),
        List())
  }
  trait SRDDImplCompanion
  // elem for concrete class
  class SRDDImplElem[A](val iso: Iso[SRDDImplData[A], SRDDImpl[A]])(implicit eA: Elem[A])
    extends SRDDElem[A, SRDDImpl[A]]
    with ConcreteElem1[A, SRDDImplData[A], SRDDImpl[A], SRDD] {
    lazy val eTo = this
    override def convertSRDD(x: Rep[SRDD[A]]) = SRDDImpl(x.wrappedValueOfBaseType)
    override def getDefaultRep = super[ConcreteElem1].getDefaultRep
    override lazy val tag = {
      implicit val tagA = eA.tag
      weakTypeTag[SRDDImpl[A]]
    }
  }

  // state representation type
  type SRDDImplData[A] = RDD[A]

  // 3) Iso for concrete class
  class SRDDImplIso[A](implicit eA: Elem[A])
    extends Iso[SRDDImplData[A], SRDDImpl[A]] {
    override def from(p: Rep[SRDDImpl[A]]) =
      p.wrappedValueOfBaseType
    override def to(p: Rep[RDD[A]]) = {
      val wrappedValueOfBaseType = p
      SRDDImpl(wrappedValueOfBaseType)
    }
    lazy val tag = {
      implicit val tagA = eA.tag
      weakTypeTag[SRDDImpl[A]]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[SRDDImpl[A]]](SRDDImpl(DefaultOfRDD[A].value))
    //lazy val defaultRepTo: Rep[SRDDImpl[A]] = SRDDImpl(DefaultOfRDD[A].value) //generated
    lazy val eTo = new SRDDImplElem[A](this)
  }
  // 4) constructor and deconstructor
  abstract class SRDDImplCompanionAbs extends CompanionBase[SRDDImplCompanionAbs] with SRDDImplCompanion {
    override def toString = "SRDDImpl"

    def apply[A](wrappedValueOfBaseType: Rep[RDD[A]])(implicit eA: Elem[A]): Rep[SRDDImpl[A]] =
      mkSRDDImpl(wrappedValueOfBaseType)
  }
  object SRDDImplMatcher {
    def unapply[A](p: Rep[SRDD[A]]) = unmkSRDDImpl(p)
  }
  def SRDDImpl: Rep[SRDDImplCompanionAbs]
  implicit def proxySRDDImplCompanion(p: Rep[SRDDImplCompanionAbs]): SRDDImplCompanionAbs = {
    proxyOps[SRDDImplCompanionAbs](p)
  }

  implicit case object SRDDImplCompanionElem extends CompanionElem[SRDDImplCompanionAbs] {
    lazy val tag = weakTypeTag[SRDDImplCompanionAbs]
    protected def getDefaultRep = SRDDImpl
  }

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
  def unmkSRDDImpl[A](p: Rep[SRDD[A]]): Option[(Rep[RDD[A]])]
}

// Seq -----------------------------------
trait RDDsSeq extends RDDsDsl with ScalanCommunityDslSeq {
  self: SparkDslSeq =>
  lazy val SRDD: Rep[SRDDCompanionAbs] = new SRDDCompanionAbs with UserTypeSeq[SRDDCompanionAbs] {
    lazy val selfType = element[SRDDCompanionAbs]
  }

    // override proxy if we deal with TypeWrapper
  //override def proxyRDD[A:Elem](p: Rep[RDD[A]]): SRDD[A] =
  //  proxyOpsEx[RDD[A],SRDD[A], SeqSRDDImpl[A]](p, bt => SeqSRDDImpl(bt))

    implicit def rDDElement[A:Elem]: Elem[RDD[A]] =
      new SeqBaseElemEx1[A, SRDD[A], RDD](
           element[SRDD[A]])(element[A], container[RDD], DefaultOfRDD[A])

  case class SeqSRDDImpl[A]
      (override val wrappedValueOfBaseType: Rep[RDD[A]])
      (implicit eA: Elem[A])
    extends SRDDImpl[A](wrappedValueOfBaseType)
        with UserTypeSeq[SRDDImpl[A]] {
    lazy val selfType = element[SRDDImpl[A]]
    override def map[B:Elem](f: Rep[A => B]): Rep[SRDD[B]] =
      SRDDImpl(wrappedValueOfBaseType.map[B](f))

    override def filter(f: Rep[A => Boolean]): Rep[SRDD[A]] =
      SRDDImpl(wrappedValueOfBaseType.filter(f))

    override def flatMap[B:Elem](f: Rep[A => SSeq[B]]): Rep[SRDD[B]] =
      SRDDImpl(wrappedValueOfBaseType.flatMap[B](in => f(in).wrappedValueOfBaseType))
      //SRDDImpl(wrappedValueOfBaseType.flatMap[B](f)) //autogenerated

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

    override def zipSafe[B:Elem](other: Rep[SRDD[B]]): Rep[SRDD[(A, B)]] =
      ??? //SRDDImpl(wrappedValueOfBaseType.zipSafe[B](other))

    override def zipWithIndex: Rep[SRDD[(A, Long)]] =
      SRDDImpl(wrappedValueOfBaseType.zipWithIndex)

    override def cache: Rep[SRDD[A]] =
      SRDDImpl(wrappedValueOfBaseType.cache)

    override def unpersist(blocking: Rep[Boolean]): Rep[SRDD[A]] =
      SRDDImpl(wrappedValueOfBaseType.unpersist(blocking))

    override def first: Rep[A] =
      wrappedValueOfBaseType.first

    override def count: Rep[Long] =
      wrappedValueOfBaseType.count

    override def collect: Rep[Array[A]] =
      wrappedValueOfBaseType.collect

    override def context: Rep[SSparkContext] =
      wrappedValueOfBaseType.context
  }
  lazy val SRDDImpl = new SRDDImplCompanionAbs with UserTypeSeq[SRDDImplCompanionAbs] {
    lazy val selfType = element[SRDDImplCompanionAbs]
  }

  def mkSRDDImpl[A]
      (wrappedValueOfBaseType: Rep[RDD[A]])(implicit eA: Elem[A]): Rep[SRDDImpl[A]] =
      new SeqSRDDImpl[A](wrappedValueOfBaseType)
  def unmkSRDDImpl[A](p: Rep[SRDD[A]]) = p match {
    case p: SRDDImpl[A] @unchecked =>
      Some((p.wrappedValueOfBaseType))
    case _ => None
  }

  implicit def wrapRDDToSRDD[A:Elem](v: RDD[A]): SRDD[A] = SRDDImpl(v)
}

// Exp -----------------------------------
trait RDDsExp extends RDDsDsl with ScalanCommunityDslExp {
  self: SparkDslExp =>
  lazy val SRDD: Rep[SRDDCompanionAbs] = new SRDDCompanionAbs with UserTypeDef[SRDDCompanionAbs] {
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

  implicit def rDDElement[A:Elem]: Elem[RDD[A]] =
      new ExpBaseElemEx1[A, SRDD[A], RDD](
           element[SRDD[A]])(element[A], container[RDD], DefaultOfRDD[A])

  case class ExpSRDDImpl[A]
      (override val wrappedValueOfBaseType: Rep[RDD[A]])
      (implicit eA: Elem[A])
    extends SRDDImpl[A](wrappedValueOfBaseType) with UserTypeDef[SRDDImpl[A]] {
    lazy val selfType = element[SRDDImpl[A]]
    override def mirror(t: Transformer) = ExpSRDDImpl[A](t(wrappedValueOfBaseType))
  }

  lazy val SRDDImpl: Rep[SRDDImplCompanionAbs] = new SRDDImplCompanionAbs with UserTypeDef[SRDDImplCompanionAbs] {
    lazy val selfType = element[SRDDImplCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object SRDDImplMethods {
  }

  def mkSRDDImpl[A]
    (wrappedValueOfBaseType: Rep[RDD[A]])(implicit eA: Elem[A]): Rep[SRDDImpl[A]] =
    new ExpSRDDImpl[A](wrappedValueOfBaseType)
  def unmkSRDDImpl[A](p: Rep[SRDD[A]]) = p.elem.asInstanceOf[Elem[_]] match {
    case _: SRDDImplElem[A] @unchecked =>
      Some((p.asRep[SRDDImpl[A]].wrappedValueOfBaseType))
    case _ =>
      None
  }

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

    object zipSafe {
      def unapply(d: Def[_]): Option[(Rep[SRDD[A]], Rep[SRDD[B]]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(other, _*), _) if receiver.elem.isInstanceOf[SRDDElem[_, _]] && method.getName == "zipSafe" =>
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

    object unpersist {
      def unapply(d: Def[_]): Option[(Rep[SRDD[A]], Rep[Boolean]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(blocking, _*), _) if receiver.elem.isInstanceOf[SRDDElem[_, _]] && method.getName == "unpersist" =>
          Some((receiver, blocking)).asInstanceOf[Option[(Rep[SRDD[A]], Rep[Boolean]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SRDD[A]], Rep[Boolean]) forSome {type A}] = exp match {
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

    object context {
      def unapply(d: Def[_]): Option[Rep[SRDD[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SRDDElem[_, _]] && method.getName == "context" =>
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
        case MethodCall(receiver, method, Seq(arr, _*), _) if receiver.elem == SRDDCompanionElem && method.getName == "apply" =>
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
        case MethodCall(receiver, method, Seq(arr, _*), _) if receiver.elem == SRDDCompanionElem && method.getName == "fromArray" =>
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
        case MethodCall(receiver, method, _, _) if receiver.elem == SRDDCompanionElem && method.getName == "empty" =>
          Some(()).asInstanceOf[Option[Unit forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Unit forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object fromArraySC {
      def unapply(d: Def[_]): Option[(Rep[SSparkContext], Rep[Array[A]]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(sc, arr, _*), _) if receiver.elem == SRDDCompanionElem && method.getName == "fromArraySC" =>
          Some((sc, arr)).asInstanceOf[Option[(Rep[SSparkContext], Rep[Array[A]]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SSparkContext], Rep[Array[A]]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object emptySC {
      def unapply(d: Def[_]): Option[Rep[SSparkContext] forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(sc, _*), _) if receiver.elem == SRDDCompanionElem && method.getName == "emptySC" =>
          Some(sc).asInstanceOf[Option[Rep[SSparkContext] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SSparkContext] forSome {type A}] = exp match {
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
    //case SRDDMethods.map(xs, Def(IdentityLambda())) => xs // generated
    case SRDDMethods.map(xs, Def(l: Lambda[_, _])) if l.isIdentity => xs
    case SRDDMethods.context(HasViews(source, contIso: SRDDIso[a,b])) =>
      source.asRep[SRDD[a]].context

    case SRDDMethods.zip(Def(v1:ViewSRDD[a,_]), rdd2: RepRDD[b] @unchecked) =>
      implicit val eA = v1.source.elem.eItem
      implicit val eB = rdd2.elem.eItem
      val iso2 = identityIso(eB)
      val pIso = SRDDIso(pairIso(v1.innerIso, iso2))
      val zipped = v1.source zip rdd2
      ViewSRDD(zipped)(pIso)
    case SRDDMethods.zip(rdd1: RepRDD[b] @unchecked, Def(v2:ViewSRDD[a,_])) =>
      implicit val eB = v2.source.elem.eItem
      implicit val eA = rdd1.elem.eItem
      val iso1 = identityIso(eA)
      val pIso = SRDDIso(pairIso(iso1, v2.innerIso))
      val zipped = rdd1 zip v2.source
      ViewSRDD(zipped)(pIso)
    case SRDDMethods.zipWithIndex(Def(v1:ViewSRDD[a,_])) =>
      implicit val eA = v1.source.elem.eItem
      val iso2 = identityIso(element[Long])
      val pIso = SRDDIso(pairIso(v1.innerIso, iso2))
      val zipped = v1.source.zipWithIndex
      ViewSRDD(zipped)(pIso)

    case view1@ViewSRDD(Def(view2@ViewSRDD(arr))) =>
      val compIso = composeIso(view1.innerIso, view2.innerIso)
      implicit val eAB = compIso.eTo
      ViewSRDD(arr)(SRDDIso(compIso))

    // Rule: W(a).m(args) ==> iso.to(a.m(unwrap(args)))
    case mc @ MethodCall(Def(wrapper: ExpSRDDImpl[_]), m, args, neverInvoke) if !isValueAccessor(m) =>
      val resultElem = mc.selfType
      val wrapperIso = getIsoByElem(resultElem)
      wrapperIso match {
        case iso: Iso[base,ext] =>
          val eRes = iso.eFrom
          val newCall = unwrapMethodCall(mc, wrapper.wrappedValueOfBaseType, eRes)
          iso.to(newCall)
      }


    case SRDDMethods.map(xs, f) => (xs, f) match {
      case (xs: RepRDD[a] @unchecked, LambdaResultHasViews(f, iso: Iso[b, c])) =>
        val f1 = f.asRep[a => c]
        implicit val eA = xs.elem.eItem
        implicit val eB = iso.eFrom
        val s = xs.map(fun { x =>
          val tmp = f1(x)
          iso.from(tmp)
        })
        val res = ViewSRDD(s)(SRDDIso(iso))
        res
      case (HasViews(source, contIso: SRDDIso[a, b]/*Iso[a,RDDsExp.this.SRDD[b]]*/), f: Rep[Function1[_, c] @unchecked]) =>  // todo make automatic SRDDIso[a, b] from Iso[Source,RDDsExp.this.SRDD[A]]
        val f1 = f.asRep[b => c]
        val iso = contIso.iso
        implicit val eA = iso.eFrom
        implicit val eB = iso.eTo
        implicit val eC = f1.elem.eRange
        source.asRep[SRDD[a]].map(fun { x => f1(iso.to(x)) })
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
        val eR = ff.elem.eRange
        implicit val eC =  (eR match {
          case bEl: BaseElemEx1[_,_,SSeq @unchecked] => bEl.eItem
          case sEl: SSeqElem[_,_] => sEl.eA
          case _ => !!!
        }).asInstanceOf[Element[c]]
        view.source.flatMap(fun { x => ff(iso.to(x))})
      }
      case _ =>
        super.rewriteDef(d)
    }

    case SRDDMethods.collect(Def(view: ViewSRDD[_,_])) => {
      val iso = view.innerIso
      ViewArray(view.source.collect)(ArrayIso(iso))
    }

    case SRDDMethods.cache(HasViews(source, rddIso: SRDDIso[a, b])) => {
      ViewSRDD(source.asRep[SRDD[a]].cache)(rddIso)
    }

    // TODO: Move to ScalantLite
    case SSeqCompanionMethods.apply(HasViews(source, arrIso: ArrayIso[a,b])) => {
      val iso = arrIso.iso
      implicit val eA = iso.eFrom
      ViewSSeq(SSeq[a](source.asRep[Array[a]])(eA))(SSeqIso(iso))
    }
    case v1@PairView(Def(v2@PairView(source))) => {
      val i1 = v1.iso.asInstanceOf[PairIso[Any,Any,Any,Any]]
      val i2 = v2.iso.asInstanceOf[PairIso[Any,Any,Any,Any]]
      val pIso1 = composeIso(i1.iso1,i2.iso1)
      val pIso2 = composeIso(i1.iso2, i2.iso2)
      PairView(source)(pIso1, pIso2)
    }
    case IfThenElse(cond, HasViews(source1, iso1: Iso[a,b]), HasViews(source2, iso2)) if ((iso1.eTo == iso2.eTo) && (iso1.eFrom == iso2.eFrom)) =>
    {
      val unviewed = IF(cond) THEN (source1.asRep[a]) ELSE (source2.asRep[a])
      iso1.to(unviewed.asRep[a])
    }
    /*case Tup(Def(Tup(Def(IfThenElse(c1, t1, e1)), sec1)), Def(Tup(Def(IfThenElse(c2, t2, e2)), sec2))) if c1 == c2 => {
      val ifFused = IF (c1) THEN { Pair(t1, t2) } ELSE { Pair(e1, e2) }
      Pair(Pair(ifFused._1, sec1), Pair(ifFused._2,sec2))
    } */

    case view1@ViewSRDD(Def(view2@ViewSRDD(arr))) =>
      //val compIso = composeIso(view1.innerIso, view2.innerIso)
      val iso1 = view1.innerIso
      val iso2 = view2.innerIso

      val compIso = if (iso1.isInstanceOf[PairIso[_,_,_,_]] && iso2.isInstanceOf[PairIso[_,_,_,_]]) {
        val i1 = iso1.asInstanceOf[PairIso[Any,Any,Any,Any]]
        val i2 = iso2.asInstanceOf[PairIso[Any,Any, Any, Any]]
        PairIso(composeIso(i1.iso1, i2.iso1), composeIso(i1.iso2, i2.iso2)).asInstanceOf[Iso[Any,Any]]
      }
      else composeIso(iso1, iso2)

      implicit val eAB = compIso.eTo
      ViewSRDD(arr)(SRDDIso(compIso))


    case _ => super.rewriteDef(d)
  }
}
