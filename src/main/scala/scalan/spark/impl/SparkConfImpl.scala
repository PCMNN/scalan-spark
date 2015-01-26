package scalan.spark
package impl

import scalan._
import org.apache.spark.SparkConf
import scala.reflect.runtime.universe._
import scalan.common.Default

trait SparkConfsAbs extends Scalan with SparkConfs
{ self: SparkConfsDsl =>
  // single proxy for each type family
  implicit def proxySSparkConf(p: Rep[SSparkConf]): SSparkConf =
    proxyOps[SSparkConf](p)
  // BaseTypeEx proxy
  implicit def proxySparkConf(p: Rep[SparkConf]): SSparkConf =
    proxyOps[SSparkConf](p.asRep[SSparkConf])
  implicit lazy val SparkConfElement: Elem[SparkConf] = new BaseElemEx[SparkConf, SSparkConf](element[SSparkConf])
  implicit lazy val DefaultOfSparkConf: Default[SparkConf] = SSparkConf.defaultVal

  abstract class SSparkConfElem[From, To <: SSparkConf](iso: Iso[From, To]) extends ViewElem[From, To]()(iso)

  trait SSparkConfCompanionElem extends CompanionElem[SSparkConfCompanionAbs]
  implicit lazy val SSparkConfCompanionElem: SSparkConfCompanionElem = new SSparkConfCompanionElem {
    lazy val tag = typeTag[SSparkConfCompanionAbs]
    lazy val defaultRep = Default.defaultVal(SSparkConf)
  }

  abstract class SSparkConfCompanionAbs extends CompanionBase[SSparkConfCompanionAbs] with SSparkConfCompanion {
    override def toString = "SSparkConf"
  }
  def SSparkConf: Rep[SSparkConfCompanionAbs]
  implicit def proxySSparkConfCompanion(p: Rep[SSparkConfCompanion]): SSparkConfCompanion = {
    proxyOps[SSparkConfCompanion](p)
  }


}

trait SparkConfsSeq extends SparkConfsAbs with SparkConfsDsl with ScalanSeq {
  lazy val SSparkConf: Rep[SSparkConfCompanionAbs] = new SSparkConfCompanionAbs with UserTypeSeq[SSparkConfCompanionAbs, SSparkConfCompanionAbs] {
    lazy val selfType = element[SSparkConfCompanionAbs]
  }


}

trait SparkConfsExp extends SparkConfsAbs with SparkConfsDsl with ScalanExp {
  lazy val SSparkConf: Rep[SSparkConfCompanionAbs] = new SSparkConfCompanionAbs with UserTypeDef[SSparkConfCompanionAbs, SSparkConfCompanionAbs] {
    lazy val selfType = element[SSparkConfCompanionAbs]
    override def mirror(t: Transformer) = this
  }



  object SSparkConfMethods {
    object setAppName {
      def unapply(d: Def[_]): Option[(Rep[SSparkConf], Rep[String])] = d match {
        case MethodCall(receiver, method, Seq(name, _*)) if receiver.elem.isInstanceOf[SSparkConfElem[_, _]] && method.getName == "setAppName" =>
          Some((receiver, name)).asInstanceOf[Option[(Rep[SSparkConf], Rep[String])]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SSparkConf], Rep[String])] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object setMaster {
      def unapply(d: Def[_]): Option[(Rep[SSparkConf], Rep[String])] = d match {
        case MethodCall(receiver, method, Seq(master, _*)) if receiver.elem.isInstanceOf[SSparkConfElem[_, _]] && method.getName == "setMaster" =>
          Some((receiver, master)).asInstanceOf[Option[(Rep[SSparkConf], Rep[String])]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SSparkConf], Rep[String])] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object set {
      def unapply(d: Def[_]): Option[(Rep[SSparkConf], Rep[String], Rep[String])] = d match {
        case MethodCall(receiver, method, Seq(key, value, _*)) if receiver.elem.isInstanceOf[SSparkConfElem[_, _]] && method.getName == "set" =>
          Some((receiver, key, value)).asInstanceOf[Option[(Rep[SSparkConf], Rep[String], Rep[String])]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SSparkConf], Rep[String], Rep[String])] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }


}
