package scalan.spark
package impl

import scalan._
import scalan.common.Default
import org.apache.spark.broadcast.Broadcast
import scala.reflect._
import scala.reflect.runtime.universe._

// Abs -----------------------------------
trait BroadcastsAbs extends Broadcasts with ScalanCommunityDsl {
  self: SparkDsl =>

  // single proxy for each type family
  implicit def proxySBroadcast[A](p: Rep[SBroadcast[A]]): SBroadcast[A] = {
    proxyOps[SBroadcast[A]](p)(scala.reflect.classTag[SBroadcast[A]])
  }

  // TypeWrapper proxy
  //implicit def proxyBroadcast[A:Elem](p: Rep[Broadcast[A]]): SBroadcast[A] =
  //  proxyOps[SBroadcast[A]](p.asRep[SBroadcast[A]])

  implicit def unwrapValueOfSBroadcast[A](w: Rep[SBroadcast[A]]): Rep[Broadcast[A]] = w.wrappedValueOfBaseType

  implicit def broadcastElement[A:Elem]: Elem[Broadcast[A]]

  implicit def castSBroadcastElement[A](elem: Elem[SBroadcast[A]]): SBroadcastElem[A, SBroadcast[A]] = elem.asInstanceOf[SBroadcastElem[A, SBroadcast[A]]]

  implicit val containerBroadcast: Cont[Broadcast] = new Container[Broadcast] {
    def tag[A](implicit evA: WeakTypeTag[A]) = weakTypeTag[Broadcast[A]]
    def lift[A](implicit evA: Elem[A]) = element[Broadcast[A]]
  }

  implicit val containerSBroadcast: Cont[SBroadcast] = new Container[SBroadcast] {
    def tag[A](implicit evA: WeakTypeTag[A]) = weakTypeTag[SBroadcast[A]]
    def lift[A](implicit evA: Elem[A]) = element[SBroadcast[A]]
  }
  case class SBroadcastIso[A,B](iso: Iso[A,B]) extends Iso1[A, B, SBroadcast](iso) {
    implicit val eA = iso.eFrom
    implicit val eB = iso.eTo
    def from(x: Rep[SBroadcast[B]]) = x.map(iso.from _)
    def to(x: Rep[SBroadcast[A]]) = x.map(iso.to _)
    lazy val defaultRepTo = SBroadcast.empty[B]
  }

  // familyElem
  abstract class SBroadcastElem[A, To <: SBroadcast[A]](implicit val eA: Elem[A])
    extends WrapperElem1[A, To, Broadcast, SBroadcast]()(eA, container[Broadcast], container[SBroadcast]) {
    override def isEntityType = true
    override lazy val tag = {
      implicit val tagA = eA.tag
      weakTypeTag[SBroadcast[A]].asInstanceOf[WeakTypeTag[To]]
    }
    override def convert(x: Rep[Reifiable[_]]) = {
      implicit val eTo: Elem[To] = this
      val conv = fun {x: Rep[SBroadcast[A]] => convertSBroadcast(x) }
      tryConvert(element[SBroadcast[A]], this, x, conv)
    }

    def convertSBroadcast(x : Rep[SBroadcast[A]]): Rep[To] = {
      assert(x.selfType1 match { case _: SBroadcastElem[_, _] => true; case _ => false })
      x.asRep[To]
    }
    override def getDefaultRep: Rep[To] = ???
  }

  implicit def sBroadcastElement[A](implicit eA: Elem[A]): Elem[SBroadcast[A]] =
    new SBroadcastElem[A, SBroadcast[A]] {
      lazy val eTo = element[SBroadcastImpl[A]]
    }

  implicit case object SBroadcastCompanionElem extends CompanionElem[SBroadcastCompanionAbs] {
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
    with ConcreteElem1[A, SBroadcastImplData[A], SBroadcastImpl[A], SBroadcast] {
    lazy val eTo = this
    override def convertSBroadcast(x: Rep[SBroadcast[A]]) = SBroadcastImpl(x.wrappedValueOfBaseType)
    override def getDefaultRep = super[ConcreteElem1].getDefaultRep
    override lazy val tag = {
      implicit val tagA = eA.tag
      weakTypeTag[SBroadcastImpl[A]]
    }
  }

  // state representation type
  type SBroadcastImplData[A] = Broadcast[A]

  // 3) Iso for concrete class
  class SBroadcastImplIso[A](implicit eA: Elem[A])
    extends Iso[SBroadcastImplData[A], SBroadcastImpl[A]] {
    override def from(p: Rep[SBroadcastImpl[A]]) =
      p.wrappedValueOfBaseType
    override def to(p: Rep[Broadcast[A]]) = {
      val wrappedValueOfBaseType = p
      SBroadcastImpl(wrappedValueOfBaseType)
    }
    lazy val defaultRepTo: Rep[SBroadcastImpl[A]] = SBroadcastImpl(DefaultOfBroadcast[A].value)
    lazy val eTo = new SBroadcastImplElem[A](this)
  }
  // 4) constructor and deconstructor
  abstract class SBroadcastImplCompanionAbs extends CompanionBase[SBroadcastImplCompanionAbs] with SBroadcastImplCompanion {
    override def toString = "SBroadcastImpl"

    def apply[A](wrappedValueOfBaseType: Rep[Broadcast[A]])(implicit eA: Elem[A]): Rep[SBroadcastImpl[A]] =
      mkSBroadcastImpl(wrappedValueOfBaseType)
  }
  object SBroadcastImplMatcher {
    def unapply[A](p: Rep[SBroadcast[A]]) = unmkSBroadcastImpl(p)
  }
  def SBroadcastImpl: Rep[SBroadcastImplCompanionAbs]
  implicit def proxySBroadcastImplCompanion(p: Rep[SBroadcastImplCompanionAbs]): SBroadcastImplCompanionAbs = {
    proxyOps[SBroadcastImplCompanionAbs](p)
  }

  implicit case object SBroadcastImplCompanionElem extends CompanionElem[SBroadcastImplCompanionAbs] {
    lazy val tag = weakTypeTag[SBroadcastImplCompanionAbs]
    protected def getDefaultRep = SBroadcastImpl
  }

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
  def unmkSBroadcastImpl[A](p: Rep[SBroadcast[A]]): Option[(Rep[Broadcast[A]])]
}

// Seq -----------------------------------
trait BroadcastsSeq extends BroadcastsDsl with ScalanCommunityDslSeq {
  self: SparkDslSeq =>
  lazy val SBroadcast: Rep[SBroadcastCompanionAbs] = new SBroadcastCompanionAbs with UserTypeSeq[SBroadcastCompanionAbs] {
    lazy val selfType = element[SBroadcastCompanionAbs]
  }

    // override proxy if we deal with TypeWrapper
  //override def proxyBroadcast[A:Elem](p: Rep[Broadcast[A]]): SBroadcast[A] =
  //  proxyOpsEx[Broadcast[A],SBroadcast[A], SeqSBroadcastImpl[A]](p, bt => SeqSBroadcastImpl(bt))

    implicit def broadcastElement[A:Elem]: Elem[Broadcast[A]] =
      new SeqBaseElemEx1[A, SBroadcast[A], Broadcast](
           element[SBroadcast[A]])(element[A], container[Broadcast], DefaultOfBroadcast[A])

  case class SeqSBroadcastImpl[A]
      (override val wrappedValueOfBaseType: Rep[Broadcast[A]])
      (implicit eA: Elem[A])
    extends SBroadcastImpl[A](wrappedValueOfBaseType)
        with UserTypeSeq[SBroadcastImpl[A]] {
    lazy val selfType = element[SBroadcastImpl[A]]
    override def value: Rep[A] =
      wrappedValueOfBaseType.value
  }
  lazy val SBroadcastImpl = new SBroadcastImplCompanionAbs with UserTypeSeq[SBroadcastImplCompanionAbs] {
    lazy val selfType = element[SBroadcastImplCompanionAbs]
  }

  def mkSBroadcastImpl[A]
      (wrappedValueOfBaseType: Rep[Broadcast[A]])(implicit eA: Elem[A]): Rep[SBroadcastImpl[A]] =
      new SeqSBroadcastImpl[A](wrappedValueOfBaseType)
  def unmkSBroadcastImpl[A](p: Rep[SBroadcast[A]]) = p match {
    case p: SBroadcastImpl[A] @unchecked =>
      Some((p.wrappedValueOfBaseType))
    case _ => None
  }

  implicit def wrapBroadcastToSBroadcast[A:Elem](v: Broadcast[A]): SBroadcast[A] = SBroadcastImpl(v)
}

// Exp -----------------------------------
trait BroadcastsExp extends BroadcastsDsl with ScalanCommunityDslExp {
  self: SparkDslExp =>
  lazy val SBroadcast: Rep[SBroadcastCompanionAbs] = new SBroadcastCompanionAbs with UserTypeDef[SBroadcastCompanionAbs] {
    lazy val selfType = element[SBroadcastCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  case class ViewSBroadcast[A, B](source: Rep[SBroadcast[A]])(iso: Iso1[A, B, SBroadcast])
    extends View1[A, B, SBroadcast](iso) {
    def copy(source: Rep[SBroadcast[A]]) = ViewSBroadcast(source)(iso)
    override def toString = s"ViewSBroadcast[${innerIso.eTo.name}]($source)"
    override def equals(other: Any) = other match {
      case v: ViewSBroadcast[_, _] => source == v.source && innerIso.eTo == v.innerIso.eTo
      case _ => false
    }
  }

  implicit def broadcastElement[A:Elem]: Elem[Broadcast[A]] =
      new ExpBaseElemEx1[A, SBroadcast[A], Broadcast](
           element[SBroadcast[A]])(element[A], container[Broadcast], DefaultOfBroadcast[A])

  case class ExpSBroadcastImpl[A]
      (override val wrappedValueOfBaseType: Rep[Broadcast[A]])
      (implicit eA: Elem[A])
    extends SBroadcastImpl[A](wrappedValueOfBaseType) with UserTypeDef[SBroadcastImpl[A]] {
    lazy val selfType = element[SBroadcastImpl[A]]
    override def mirror(t: Transformer) = ExpSBroadcastImpl[A](t(wrappedValueOfBaseType))
  }

  lazy val SBroadcastImpl: Rep[SBroadcastImplCompanionAbs] = new SBroadcastImplCompanionAbs with UserTypeDef[SBroadcastImplCompanionAbs] {
    lazy val selfType = element[SBroadcastImplCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object SBroadcastImplMethods {
  }

  def mkSBroadcastImpl[A]
    (wrappedValueOfBaseType: Rep[Broadcast[A]])(implicit eA: Elem[A]): Rep[SBroadcastImpl[A]] =
    new ExpSBroadcastImpl[A](wrappedValueOfBaseType)
  def unmkSBroadcastImpl[A](p: Rep[SBroadcast[A]]) = p.elem.asInstanceOf[Elem[_]] match {
    case _: SBroadcastImplElem[A] @unchecked =>
      Some((p.asRep[SBroadcastImpl[A]].wrappedValueOfBaseType))
    case _ =>
      None
  }

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
        case MethodCall(receiver, method, _, _) if receiver.elem == SBroadcastCompanionElem && method.getName == "empty" =>
          Some(()).asInstanceOf[Option[Unit forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Unit forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object UserTypeSBroadcast {
    def unapply(s: Exp[_]): Option[Iso[_, _]] = {
      s.elem match {
        case e: SBroadcastElem[a,to] => e.eItem match {
          case UnpackableElem(iso) => Some(iso)
          case _ => None
        }
        case _ => None
      }
    }
  }

  override def unapplyViews[T](s: Exp[T]): Option[Unpacked[T]] = (s match {
    case Def(view: ViewSBroadcast[_, _]) =>
      Some((view.source, view.iso))
    case UserTypeSBroadcast(iso: Iso[a, b]) =>
      val newIso = SBroadcastIso(iso)
      val repr = reifyObject(UnpackView(s.asRep[SBroadcast[b]])(newIso))
      Some((repr, newIso))
    case _ =>
      super.unapplyViews(s)
  }).asInstanceOf[Option[Unpacked[T]]]

  override def rewriteDef[T](d: Def[T]) = d match {
    case SBroadcastMethods.map(xs, Def(IdentityLambda())) => xs

    case view1@ViewSBroadcast(Def(view2@ViewSBroadcast(arr))) =>
      val compIso = composeIso(view1.innerIso, view2.innerIso)
      implicit val eAB = compIso.eTo
      ViewSBroadcast(arr)(SBroadcastIso(compIso))

    // Rule: W(a).m(args) ==> iso.to(a.m(unwrap(args)))
    case mc @ MethodCall(Def(wrapper: ExpSBroadcastImpl[_]), m, args, neverInvoke) if !isValueAccessor(m) =>
      val resultElem = mc.selfType
      val wrapperIso = getIsoByElem(resultElem)
      wrapperIso match {
        case iso: Iso[base,ext] =>
          val eRes = iso.eFrom
          val newCall = unwrapMethodCall(mc, wrapper.wrappedValueOfBaseType, eRes)
          iso.to(newCall)
      }

    case SBroadcastMethods.map(xs, f) => (xs, f) match {
      case (xs: RepBroadcast[a] @unchecked, LambdaResultHasViews(f, iso: Iso[b, c])) =>
        val f1 = f.asRep[a => c]
        implicit val eA = xs.elem.eItem
        implicit val eB = iso.eFrom
        val s = xs.map(fun { x =>
          val tmp = f1(x)
          iso.from(tmp)
        })
        val res = ViewSBroadcast(s)(SBroadcastIso(iso))
        res
      case (HasViews(source, contIso: SBroadcastIso[a, b]), f: Rep[Function1[_, c] @unchecked]) =>
        val f1 = f.asRep[b => c]
        val iso = contIso.iso
        implicit val eA = iso.eFrom
        implicit val eB = iso.eTo
        implicit val eC = f1.elem.eRange
        source.asRep[SBroadcast[a]].map(fun { x => f1(iso.to(x)) })
      case _ =>
        super.rewriteDef(d)
    }

    case _ => super.rewriteDef(d)
  }
}
