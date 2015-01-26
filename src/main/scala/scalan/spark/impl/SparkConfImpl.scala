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

  implicit def defaultSSparkConfElem: Elem[SSparkConf] = element[SSparkConfImpl].asElem[SSparkConf]
  implicit def SparkConfElement: Elem[SparkConf]

  abstract class SSparkConfElem[From, To <: SSparkConf](iso: Iso[From, To]) extends ViewElem[From, To]()(iso)

  trait SSparkConfCompanionElem extends CompanionElem[SSparkConfCompanionAbs]
  implicit lazy val SSparkConfCompanionElem: SSparkConfCompanionElem = new SSparkConfCompanionElem {
    lazy val tag = typeTag[SSparkConfCompanionAbs]
    lazy val getDefaultRep = Default.defaultVal(SSparkConf)
    //def getDefaultRep = defaultRep
  }

  abstract class SSparkConfCompanionAbs extends CompanionBase[SSparkConfCompanionAbs] with SSparkConfCompanion {
    override def toString = "SSparkConf"
    
  }
  def SSparkConf: Rep[SSparkConfCompanionAbs]
  implicit def proxySSparkConfCompanion(p: Rep[SSparkConfCompanion]): SSparkConfCompanion = {
    proxyOps[SSparkConfCompanion](p)
  }

  //default wrapper implementation
    abstract class SSparkConfImpl(val value: Rep[SparkConf]) extends SSparkConf {
    
    def setAppName(name: Rep[String]): RepSparkConf =
      methodCallEx[SSparkConf](self,
        this.getClass.getMethod("setAppName", classOf[AnyRef]),
        List(name.asInstanceOf[AnyRef]))

    
    def setMaster(master: Rep[String]): RepSparkConf =
      methodCallEx[SSparkConf](self,
        this.getClass.getMethod("setMaster", classOf[AnyRef]),
        List(master.asInstanceOf[AnyRef]))

    
    def set(key: Rep[String], value: Rep[String]): RepSparkConf =
      methodCallEx[SSparkConf](self,
        this.getClass.getMethod("set", classOf[AnyRef], classOf[AnyRef]),
        List(key.asInstanceOf[AnyRef], value.asInstanceOf[AnyRef]))

  }
  trait SSparkConfImplCompanion
  // elem for concrete class
  class SSparkConfImplElem(iso: Iso[SSparkConfImplData, SSparkConfImpl]) extends SSparkConfElem[SSparkConfImplData, SSparkConfImpl](iso)

  // state representation type
  type SSparkConfImplData = SparkConf

  // 3) Iso for concrete class
  class SSparkConfImplIso
    extends Iso[SSparkConfImplData, SSparkConfImpl] {
    override def from(p: Rep[SSparkConfImpl]) =
      unmkSSparkConfImpl(p) match {
        case Some((value)) => value
        case None => !!!
      }
    override def to(p: Rep[SparkConf]) = {
      val value = p
      SSparkConfImpl(value)
    }
    lazy val tag = {
      weakTypeTag[SSparkConfImpl]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[SSparkConfImpl]](SSparkConfImpl(Default.defaultOf[SparkConf]))
    lazy val eTo = new SSparkConfImplElem(this)
  }
  // 4) constructor and deconstructor
  abstract class SSparkConfImplCompanionAbs extends CompanionBase[SSparkConfImplCompanionAbs] with SSparkConfImplCompanion {
    override def toString = "SSparkConfImpl"

    def apply(value: Rep[SparkConf]): Rep[SSparkConfImpl] =
      mkSSparkConfImpl(value)
    def unapply(p: Rep[SSparkConfImpl]) = unmkSSparkConfImpl(p)
  }
  def SSparkConfImpl: Rep[SSparkConfImplCompanionAbs]
  implicit def proxySSparkConfImplCompanion(p: Rep[SSparkConfImplCompanionAbs]): SSparkConfImplCompanionAbs = {
    proxyOps[SSparkConfImplCompanionAbs](p)
  }

  class SSparkConfImplCompanionElem extends CompanionElem[SSparkConfImplCompanionAbs] {
    lazy val tag = typeTag[SSparkConfImplCompanionAbs]
    lazy val getDefaultRep = Default.defaultVal(SSparkConfImpl)
    //def getDefaultRep = defaultRep
  }
  implicit lazy val SSparkConfImplCompanionElem: SSparkConfImplCompanionElem = new SSparkConfImplCompanionElem

  implicit def proxySSparkConfImpl(p: Rep[SSparkConfImpl]): SSparkConfImpl =
    proxyOps[SSparkConfImpl](p)

  implicit class ExtendedSSparkConfImpl(p: Rep[SSparkConfImpl]) {
    def toData: Rep[SSparkConfImplData] = isoSSparkConfImpl.from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoSSparkConfImpl: Iso[SSparkConfImplData, SSparkConfImpl] =
    new SSparkConfImplIso

  // 6) smart constructor and deconstructor
  def mkSSparkConfImpl(value: Rep[SparkConf]): Rep[SSparkConfImpl]
  def unmkSSparkConfImpl(p: Rep[SSparkConfImpl]): Option[(Rep[SparkConf])]
}

trait SparkConfsSeq extends SparkConfsAbs with SparkConfsDsl with ScalanSeq { self: SparkConfsDslSeq =>
  lazy val SSparkConf: Rep[SSparkConfCompanionAbs] = new SSparkConfCompanionAbs with UserTypeSeq[SSparkConfCompanionAbs, SSparkConfCompanionAbs] {
    lazy val selfType = element[SSparkConfCompanionAbs]
    
  }

    // override proxy if we deal with BaseTypeEx
  override def proxySparkConf(p: Rep[SparkConf]): SSparkConf =
    proxyOpsEx[SparkConf,SSparkConf, SeqSSparkConfImpl](p, bt => SeqSSparkConfImpl(bt))

    implicit lazy val SparkConfElement: Elem[SparkConf] = new SeqBaseElemEx[SparkConf, SSparkConf](element[SSparkConf])

  case class SeqSSparkConfImpl
      (override val value: Rep[SparkConf])
      
    extends SSparkConfImpl(value)
        with UserTypeSeq[SSparkConf, SSparkConfImpl] {
    lazy val selfType = element[SSparkConfImpl].asInstanceOf[Elem[SSparkConf]]
    
    override def setAppName(name: Rep[String]): RepSparkConf =
      value.setAppName(name)

    
    override def setMaster(master: Rep[String]): RepSparkConf =
      value.setMaster(master)

    
    override def set(key: Rep[String], value: Rep[String]): RepSparkConf =
      value.set(key, value)

  }
  lazy val SSparkConfImpl = new SSparkConfImplCompanionAbs with UserTypeSeq[SSparkConfImplCompanionAbs, SSparkConfImplCompanionAbs] {
    lazy val selfType = element[SSparkConfImplCompanionAbs]
  }

  def mkSSparkConfImpl
      (value: Rep[SparkConf]) =
      new SeqSSparkConfImpl(value)
  def unmkSSparkConfImpl(p: Rep[SSparkConfImpl]) =
    Some((p.value))
}

trait SparkConfsExp extends SparkConfsAbs with SparkConfsDsl with ScalanExp {
  lazy val SSparkConf: Rep[SSparkConfCompanionAbs] = new SSparkConfCompanionAbs with UserTypeDef[SSparkConfCompanionAbs, SSparkConfCompanionAbs] {
    lazy val selfType = element[SSparkConfCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  implicit lazy val SparkConfElement: Elem[SparkConf] = new ExpBaseElemEx[SparkConf, SSparkConf](element[SSparkConf])

  case class ExpSSparkConfImpl
      (override val value: Rep[SparkConf])
      
    extends SSparkConfImpl(value) with UserTypeDef[SSparkConf, SSparkConfImpl] {
    lazy val selfType = element[SSparkConfImpl].asInstanceOf[Elem[SSparkConf]]
    override def mirror(t: Transformer) = ExpSSparkConfImpl(t(value))
  }

  lazy val SSparkConfImpl: Rep[SSparkConfImplCompanionAbs] = new SSparkConfImplCompanionAbs with UserTypeDef[SSparkConfImplCompanionAbs, SSparkConfImplCompanionAbs] {
    lazy val selfType = element[SSparkConfImplCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object SSparkConfImplMethods {

  }



  def mkSSparkConfImpl
    (value: Rep[SparkConf]) =
    new ExpSSparkConfImpl(value)
  def unmkSSparkConfImpl(p: Rep[SSparkConfImpl]) =
    Some((p.value))

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