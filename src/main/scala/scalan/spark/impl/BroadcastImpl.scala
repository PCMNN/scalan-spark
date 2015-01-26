package scalan.spark
package impl

import scalan._
import org.apache.spark.broadcast.Broadcast
import scala.reflect.runtime.universe._
import scalan.common.Default

trait BroadcastsAbs extends Scalan with Broadcasts
{ self: BroadcastsDsl =>
  // single proxy for each type family
  implicit def proxySBroadcast[A](p: Rep[SBroadcast[A]]): SBroadcast[A] =
    proxyOps[SBroadcast[A]](p)
  // BaseTypeEx proxy
  implicit def proxyBroadcast[A](p: Rep[Broadcast[A]]): SBroadcast[A] =
    proxyOps[SBroadcast[A]](p.asRep[SBroadcast[A]])
  implicit def BroadcastElement[A:Elem]: Elem[Broadcast[A]] = new BaseElemEx[Broadcast[A], SBroadcast[A]](element[SBroadcast[A]])
  implicit lazy val DefaultOfBroadcast[A:Elem]: Default[Broadcast[A]] = SBroadcast.defaultVal

  abstract class SBroadcastElem[A, From, To <: SBroadcast[A]](iso: Iso[From, To]) extends ViewElem[From, To]()(iso)

  trait SBroadcastCompanionElem extends CompanionElem[SBroadcastCompanionAbs]
  implicit lazy val SBroadcastCompanionElem: SBroadcastCompanionElem = new SBroadcastCompanionElem {
    lazy val tag = typeTag[SBroadcastCompanionAbs]
    lazy val defaultRep = Default.defaultVal(SBroadcast)
  }

  abstract class SBroadcastCompanionAbs extends CompanionBase[SBroadcastCompanionAbs] with SBroadcastCompanion {
    override def toString = "SBroadcast"
  }
  def SBroadcast: Rep[SBroadcastCompanionAbs]
  implicit def proxySBroadcastCompanion(p: Rep[SBroadcastCompanion]): SBroadcastCompanion = {
    proxyOps[SBroadcastCompanion](p)
  }


}

trait BroadcastsSeq extends BroadcastsAbs with BroadcastsDsl with ScalanSeq {
  lazy val SBroadcast: Rep[SBroadcastCompanionAbs] = new SBroadcastCompanionAbs with UserTypeSeq[SBroadcastCompanionAbs, SBroadcastCompanionAbs] {
    lazy val selfType = element[SBroadcastCompanionAbs]
  }


}

trait BroadcastsExp extends BroadcastsAbs with BroadcastsDsl with ScalanExp {
  lazy val SBroadcast: Rep[SBroadcastCompanionAbs] = new SBroadcastCompanionAbs with UserTypeDef[SBroadcastCompanionAbs, SBroadcastCompanionAbs] {
    lazy val selfType = element[SBroadcastCompanionAbs]
    override def mirror(t: Transformer) = this
  }



  object SBroadcastMethods {
    object value {
      def unapply(d: Def[_]): Option[Rep[SBroadcast[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _) if receiver.elem.isInstanceOf[SBroadcastElem[A, _, _] forSome {type A}] && method.getName == "value" =>
          Some(receiver).asInstanceOf[Option[Rep[SBroadcast[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SBroadcast[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }


}
