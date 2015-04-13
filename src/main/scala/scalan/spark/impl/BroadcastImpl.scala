package scalan.spark
package impl

import scalan._
import scalan.common.Default
import org.apache.spark.broadcast.Broadcast
import scala.reflect._
import scala.reflect.runtime.universe._
import scalan.common.Default

// Abs -----------------------------------
trait BroadcastsAbs extends ScalanCommunityDsl with Broadcasts {
  self: SparkDsl =>
  // single proxy for each type family
  implicit def proxySBroadcast[A](p: Rep[SBroadcast[A]]): SBroadcast[A] = {
    proxyOps[SBroadcast[A]](p)(classTag[SBroadcast[A]])
  }
  // BaseTypeEx proxy
  //implicit def proxyBroadcast[A:Elem](p: Rep[Broadcast[A]]): SBroadcast[A] =
  //  proxyOps[SBroadcast[A]](p.asRep[SBroadcast[A]])

  implicit def unwrapValueOfSBroadcast[A](w: Rep[SBroadcast[A]]): Rep[Broadcast[A]] = w.wrappedValueOfBaseType

  implicit def defaultSBroadcastElem[A:Elem]: Elem[SBroadcast[A]] = element[SBroadcastImpl[A]].asElem[SBroadcast[A]]
  implicit def broadcastElement[A:Elem]: Elem[Broadcast[A]]

  class SBroadcastElem[A, To <: SBroadcast[A]](implicit val eA: Elem[A])
    extends EntityElem[To] {
    override def isEntityType = true
    override def tag = {
      implicit val tagA = eA.tag
      weakTypeTag[SBroadcast[A]].asInstanceOf[WeakTypeTag[To]]
    }
    override def convert(x: Rep[Reifiable[_]]) = convertSBroadcast(x.asRep[SBroadcast[A]])
    def convertSBroadcast(x : Rep[SBroadcast[A]]): Rep[To] = {
      assert(x.selfType1.isInstanceOf[SBroadcastElem[_,_]])
      x.asRep[To]
    }
    override def getDefaultRep: Rep[To] = ???
  }

  implicit def sBroadcastElement[A](implicit eA: Elem[A]) =
    new SBroadcastElem[A, SBroadcast[A]]()(eA)

  trait SBroadcastCompanionElem extends CompanionElem[SBroadcastCompanionAbs]
  implicit lazy val SBroadcastCompanionElem: SBroadcastCompanionElem = new SBroadcastCompanionElem {
    lazy val tag = weakTypeTag[SBroadcastCompanionAbs]
    protected def getDefaultRep = SBroadcast
  }

  abstract class SBroadcastCompanionAbs extends CompanionBase[SBroadcastCompanionAbs] with SBroadcastCompanion {
    override def toString = "SBroadcast"
  }
  def SBroadcast: Rep[SBroadcastCompanionAbs]
  implicit def proxySBroadcastCompanion(p: Rep[SBroadcastCompanion]): SBroadcastCompanion = {
    proxyOps[SBroadcastCompanion](p)
  }

  // default wrapper implementation
  abstract class SBroadcastImpl[A](val wrappedValueOfBaseType: Rep[Broadcast[A]])(implicit val eA: Elem[A]) extends SBroadcast[A] {
    def value: Rep[A] =
      methodCallEx[A](self,
        this.getClass.getMethod("value"),
        List())
  }
  trait SBroadcastImplCompanion
  // elem for concrete class
  class SBroadcastImplElem[A](val iso: Iso[SBroadcastImplData[A], SBroadcastImpl[A]])(implicit eA: Elem[A])
    extends SBroadcastElem[A, SBroadcastImpl[A]]
    with ViewElem[SBroadcastImplData[A], SBroadcastImpl[A]] {
    override def convertSBroadcast(x: Rep[SBroadcast[A]]) = SBroadcastImpl(x.wrappedValueOfBaseType)
    override def getDefaultRep = super[ViewElem].getDefaultRep
    override lazy val tag = super[ViewElem].tag
  }

  // state representation type
  type SBroadcastImplData[A] = Broadcast[A]

  // 3) Iso for concrete class
  class SBroadcastImplIso[A](implicit eA: Elem[A])
    extends Iso[SBroadcastImplData[A], SBroadcastImpl[A]] {
    override def from(p: Rep[SBroadcastImpl[A]]) =
      unmkSBroadcastImpl(p) match {
        case Some((wrappedValueOfBaseType)) => wrappedValueOfBaseType
        case None => !!!
      }
    override def to(p: Rep[Broadcast[A]]) = {
      val wrappedValueOfBaseType = p
      SBroadcastImpl(wrappedValueOfBaseType)
    }
    lazy val tag = {
      implicit val tagA = eA.tag
      weakTypeTag[SBroadcastImpl[A]]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[SBroadcastImpl[A]]](SBroadcastImpl(DefaultOfBroadcast[A].value))
    lazy val eTo = new SBroadcastImplElem[A](this)
  }
  // 4) constructor and deconstructor
  abstract class SBroadcastImplCompanionAbs extends CompanionBase[SBroadcastImplCompanionAbs] with SBroadcastImplCompanion {
    override def toString = "SBroadcastImpl"

    def apply[A](wrappedValueOfBaseType: Rep[Broadcast[A]])(implicit eA: Elem[A]): Rep[SBroadcastImpl[A]] =
      mkSBroadcastImpl(wrappedValueOfBaseType)
    def unapply[A:Elem](p: Rep[SBroadcastImpl[A]]) = unmkSBroadcastImpl(p)
  }
  def SBroadcastImpl: Rep[SBroadcastImplCompanionAbs]
  implicit def proxySBroadcastImplCompanion(p: Rep[SBroadcastImplCompanionAbs]): SBroadcastImplCompanionAbs = {
    proxyOps[SBroadcastImplCompanionAbs](p)
  }

  class SBroadcastImplCompanionElem extends CompanionElem[SBroadcastImplCompanionAbs] {
    lazy val tag = weakTypeTag[SBroadcastImplCompanionAbs]
    protected def getDefaultRep = SBroadcastImpl
  }
  implicit lazy val SBroadcastImplCompanionElem: SBroadcastImplCompanionElem = new SBroadcastImplCompanionElem

  implicit def proxySBroadcastImpl[A](p: Rep[SBroadcastImpl[A]]): SBroadcastImpl[A] =
    proxyOps[SBroadcastImpl[A]](p)

  implicit class ExtendedSBroadcastImpl[A](p: Rep[SBroadcastImpl[A]])(implicit eA: Elem[A]) {
    def toData: Rep[SBroadcastImplData[A]] = isoSBroadcastImpl(eA).from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoSBroadcastImpl[A](implicit eA: Elem[A]): Iso[SBroadcastImplData[A], SBroadcastImpl[A]] =
    new SBroadcastImplIso[A]

  // 6) smart constructor and deconstructor
  def mkSBroadcastImpl[A](wrappedValueOfBaseType: Rep[Broadcast[A]])(implicit eA: Elem[A]): Rep[SBroadcastImpl[A]]
  def unmkSBroadcastImpl[A:Elem](p: Rep[SBroadcastImpl[A]]): Option[(Rep[Broadcast[A]])]
}

// Seq -----------------------------------
trait BroadcastsSeq extends BroadcastsDsl with ScalanCommunityDslSeq {
  self: SparkDslSeq =>
  lazy val SBroadcast: Rep[SBroadcastCompanionAbs] = new SBroadcastCompanionAbs with UserTypeSeq[SBroadcastCompanionAbs, SBroadcastCompanionAbs] {
    lazy val selfType = element[SBroadcastCompanionAbs]
  }

    // override proxy if we deal with BaseTypeEx
  //override def proxyBroadcast[A:Elem](p: Rep[Broadcast[A]]): SBroadcast[A] =
  //  proxyOpsEx[Broadcast[A],SBroadcast[A], SeqSBroadcastImpl[A]](p, bt => SeqSBroadcastImpl(bt))

    implicit def broadcastElement[A:Elem]: Elem[Broadcast[A]] = new SeqBaseElemEx[Broadcast[A], SBroadcast[A]](element[SBroadcast[A]])(weakTypeTag[Broadcast[A]], DefaultOfBroadcast[A])

  case class SeqSBroadcastImpl[A]
      (override val wrappedValueOfBaseType: Rep[Broadcast[A]])
      (implicit eA: Elem[A])
    extends SBroadcastImpl[A](wrappedValueOfBaseType)
        with UserTypeSeq[SBroadcast[A], SBroadcastImpl[A]] {
    lazy val selfType = element[SBroadcastImpl[A]].asInstanceOf[Elem[SBroadcast[A]]]
    override def value: Rep[A] =
      wrappedValueOfBaseType.value
  }
  lazy val SBroadcastImpl = new SBroadcastImplCompanionAbs with UserTypeSeq[SBroadcastImplCompanionAbs, SBroadcastImplCompanionAbs] {
    lazy val selfType = element[SBroadcastImplCompanionAbs]
  }

  def mkSBroadcastImpl[A]
      (wrappedValueOfBaseType: Rep[Broadcast[A]])(implicit eA: Elem[A]): Rep[SBroadcastImpl[A]] =
      new SeqSBroadcastImpl[A](wrappedValueOfBaseType)
  def unmkSBroadcastImpl[A:Elem](p: Rep[SBroadcastImpl[A]]) =
    Some((p.wrappedValueOfBaseType))

  implicit def wrapBroadcastToSBroadcast[A:Elem](v: Broadcast[A]): SBroadcast[A] = SBroadcastImpl(v)
}

// Exp -----------------------------------
trait BroadcastsExp extends BroadcastsDsl with ScalanCommunityDslExp {
  self: SparkDslExp =>
  lazy val SBroadcast: Rep[SBroadcastCompanionAbs] = new SBroadcastCompanionAbs with UserTypeDef[SBroadcastCompanionAbs, SBroadcastCompanionAbs] {
    lazy val selfType = element[SBroadcastCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  implicit def broadcastElement[A:Elem]: Elem[Broadcast[A]] = new ExpBaseElemEx[Broadcast[A], SBroadcast[A]](element[SBroadcast[A]])(weakTypeTag[Broadcast[A]], DefaultOfBroadcast[A])
  case class ExpSBroadcastImpl[A]
      (override val wrappedValueOfBaseType: Rep[Broadcast[A]])
      (implicit eA: Elem[A])
    extends SBroadcastImpl[A](wrappedValueOfBaseType) with UserTypeDef[SBroadcast[A], SBroadcastImpl[A]] {
    lazy val selfType = element[SBroadcastImpl[A]].asInstanceOf[Elem[SBroadcast[A]]]
    override def mirror(t: Transformer) = ExpSBroadcastImpl[A](t(wrappedValueOfBaseType))
  }

  lazy val SBroadcastImpl: Rep[SBroadcastImplCompanionAbs] = new SBroadcastImplCompanionAbs with UserTypeDef[SBroadcastImplCompanionAbs, SBroadcastImplCompanionAbs] {
    lazy val selfType = element[SBroadcastImplCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object SBroadcastImplMethods {
  }

  def mkSBroadcastImpl[A]
    (wrappedValueOfBaseType: Rep[Broadcast[A]])(implicit eA: Elem[A]): Rep[SBroadcastImpl[A]] =
    new ExpSBroadcastImpl[A](wrappedValueOfBaseType)
  def unmkSBroadcastImpl[A:Elem](p: Rep[SBroadcastImpl[A]]) =
    Some((p.wrappedValueOfBaseType))

  object SBroadcastMethods {
    object wrappedValueOfBaseType {
      def unapply(d: Def[_]): Option[Rep[SBroadcast[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SBroadcastElem[_, _]] && method.getName == "wrappedValueOfBaseType" =>
          Some(receiver).asInstanceOf[Option[Rep[SBroadcast[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SBroadcast[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object map {
      def unapply(d: Def[_]): Option[(Rep[SBroadcast[A]], Rep[A => B]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[SBroadcastElem[_, _]] && method.getName == "map" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[SBroadcast[A]], Rep[A => B]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SBroadcast[A]], Rep[A => B]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object value {
      def unapply(d: Def[_]): Option[Rep[SBroadcast[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SBroadcastElem[_, _]] && method.getName == "value" =>
          Some(receiver).asInstanceOf[Option[Rep[SBroadcast[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SBroadcast[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object SBroadcastCompanionMethods {
    object empty {
      def unapply(d: Def[_]): Option[Unit forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SBroadcastCompanionElem] && method.getName == "empty" =>
          Some(()).asInstanceOf[Option[Unit forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Unit forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }
}
