package scalan.spark

import scalan._
import scalan.common.Default
import org.apache.spark.SparkConf
import scala.reflect._
import scala.reflect.runtime.universe._

package impl {

import scalan.meta.ScalanAst.STraitOrClassDef

// Abs -----------------------------------
trait SparkConfsAbs extends SparkConfs with ScalanCommunityDsl {
  self: SparkDsl =>

  // single proxy for each type family
  implicit def proxySSparkConf(p: Rep[SSparkConf]): SSparkConf = {
    proxyOps[SSparkConf](p)(scala.reflect.classTag[SSparkConf])
  }

  // TypeWrapper proxy
  //implicit def proxySparkConf(p: Rep[SparkConf]): SSparkConf =
  //  proxyOps[SSparkConf](p.asRep[SSparkConf])

  implicit def unwrapValueOfSSparkConf(w: Rep[SSparkConf]): Rep[SparkConf] = w.wrappedValueOfBaseType

  implicit def sparkConfElement: Elem[SparkConf]

  // familyElem
  abstract class SSparkConfElem[To <: SSparkConf]
    extends WrapperElem[SparkConf, To] {
    lazy val parent: Option[Elem[_]] = None
    lazy val entityDef: STraitOrClassDef = {
      val module = getModules("SparkConfs")
      module.entities.find(_.name == "SSparkConf").get
    }
    lazy val tyArgSubst: Map[String, TypeDesc] = {
      Map()
    }
    override def isEntityType = true
    override lazy val tag = {
      weakTypeTag[SSparkConf].asInstanceOf[WeakTypeTag[To]]
    }
    override def convert(x: Rep[Reifiable[_]]) = {
      implicit val eTo: Elem[To] = this
      val conv = fun {x: Rep[SSparkConf] => convertSSparkConf(x) }
      tryConvert(element[SSparkConf], this, x, conv)
    }

    def convertSSparkConf(x : Rep[SSparkConf]): Rep[To] = {
      assert(x.selfType1 match { case _: SSparkConfElem[_] => true; case _ => false })
      x.asRep[To]
    }
    override def getDefaultRep: Rep[To] = ???
  }

  implicit def sSparkConfElement: Elem[SSparkConf] =
    new SSparkConfElem[SSparkConf] {
      lazy val eTo = element[SSparkConfImpl]
    }

  implicit case object SSparkConfCompanionElem extends CompanionElem[SSparkConfCompanionAbs] {
    lazy val tag = weakTypeTag[SSparkConfCompanionAbs]
    protected def getDefaultRep = SSparkConf
  }

  abstract class SSparkConfCompanionAbs extends CompanionBase[SSparkConfCompanionAbs] with SSparkConfCompanion {
    override def toString = "SSparkConf"
  }
  def SSparkConf: Rep[SSparkConfCompanionAbs]
  implicit def proxySSparkConfCompanion(p: Rep[SSparkConfCompanion]): SSparkConfCompanion =
    proxyOps[SSparkConfCompanion](p)

  // default wrapper implementation
  abstract class SSparkConfImpl(val wrappedValueOfBaseType: Rep[SparkConf]) extends SSparkConf {
    def setAppName(name: Rep[String]): Rep[SSparkConf] =
      methodCallEx[SSparkConf](self,
        this.getClass.getMethod("setAppName", classOf[AnyRef]),
        List(name.asInstanceOf[AnyRef]))

    def setMaster(master: Rep[String]): Rep[SSparkConf] =
      methodCallEx[SSparkConf](self,
        this.getClass.getMethod("setMaster", classOf[AnyRef]),
        List(master.asInstanceOf[AnyRef]))

    def set(key: Rep[String], value: Rep[String]): Rep[SSparkConf] =
      methodCallEx[SSparkConf](self,
        this.getClass.getMethod("set", classOf[AnyRef], classOf[AnyRef]),
        List(key.asInstanceOf[AnyRef], value.asInstanceOf[AnyRef]))
  }
  trait SSparkConfImplCompanion
  // elem for concrete class
  class SSparkConfImplElem(val iso: Iso[SSparkConfImplData, SSparkConfImpl])
    extends SSparkConfElem[SSparkConfImpl]
    with ConcreteElem[SSparkConfImplData, SSparkConfImpl] {
    override lazy val parent: Option[Elem[_]] = Some(sSparkConfElement)
    override lazy val entityDef = {
      val module = getModules("SparkConfs")
      module.concreteSClasses.find(_.name == "SSparkConfImpl").get
    }
    override lazy val tyArgSubst: Map[String, TypeDesc] = {
      Map()
    }
    lazy val eTo = this
    override def convertSSparkConf(x: Rep[SSparkConf]) = SSparkConfImpl(x.wrappedValueOfBaseType)
    override def getDefaultRep = super[ConcreteElem].getDefaultRep
    override lazy val tag = {
      weakTypeTag[SSparkConfImpl]
    }
  }

  // state representation type
  type SSparkConfImplData = SparkConf

  // 3) Iso for concrete class
  class SSparkConfImplIso
    extends Iso[SSparkConfImplData, SSparkConfImpl] {
    override def from(p: Rep[SSparkConfImpl]) =
      p.wrappedValueOfBaseType
    override def to(p: Rep[SparkConf]) = {
      val wrappedValueOfBaseType = p
      SSparkConfImpl(wrappedValueOfBaseType)
    }
    lazy val defaultRepTo: Rep[SSparkConfImpl] = SSparkConfImpl(DefaultOfSparkConf.value)
    lazy val eTo = new SSparkConfImplElem(this)
  }
  // 4) constructor and deconstructor
  abstract class SSparkConfImplCompanionAbs extends CompanionBase[SSparkConfImplCompanionAbs] {
    override def toString = "SSparkConfImpl"

    def apply(wrappedValueOfBaseType: Rep[SparkConf]): Rep[SSparkConfImpl] =
      mkSSparkConfImpl(wrappedValueOfBaseType)
  }
  object SSparkConfImplMatcher {
    def unapply(p: Rep[SSparkConf]) = unmkSSparkConfImpl(p)
  }
  def SSparkConfImpl: Rep[SSparkConfImplCompanionAbs]
  implicit def proxySSparkConfImplCompanion(p: Rep[SSparkConfImplCompanionAbs]): SSparkConfImplCompanionAbs = {
    proxyOps[SSparkConfImplCompanionAbs](p)
  }

  implicit case object SSparkConfImplCompanionElem extends CompanionElem[SSparkConfImplCompanionAbs] {
    lazy val tag = weakTypeTag[SSparkConfImplCompanionAbs]
    protected def getDefaultRep = SSparkConfImpl
  }

  implicit def proxySSparkConfImpl(p: Rep[SSparkConfImpl]): SSparkConfImpl =
    proxyOps[SSparkConfImpl](p)

  implicit class ExtendedSSparkConfImpl(p: Rep[SSparkConfImpl]) {
    def toData: Rep[SSparkConfImplData] = isoSSparkConfImpl.from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoSSparkConfImpl: Iso[SSparkConfImplData, SSparkConfImpl] =
    new SSparkConfImplIso

  // 6) smart constructor and deconstructor
  def mkSSparkConfImpl(wrappedValueOfBaseType: Rep[SparkConf]): Rep[SSparkConfImpl]
  def unmkSSparkConfImpl(p: Rep[SSparkConf]): Option[(Rep[SparkConf])]

  registerModule(scalan.meta.ScalanCodegen.loadModule(SparkConfs_Module.dump))
}

// Seq -----------------------------------
trait SparkConfsSeq extends SparkConfsDsl with ScalanCommunityDslSeq {
  self: SparkDslSeq =>
  lazy val SSparkConf: Rep[SSparkConfCompanionAbs] = new SSparkConfCompanionAbs with UserTypeSeq[SSparkConfCompanionAbs] {
    lazy val selfType = element[SSparkConfCompanionAbs]

    override def apply: Rep[SSparkConf] =
      SSparkConfImpl(new SparkConf)
  }

    // override proxy if we deal with TypeWrapper
  //override def proxySparkConf(p: Rep[SparkConf]): SSparkConf =
  //  proxyOpsEx[SparkConf,SSparkConf, SeqSSparkConfImpl](p, bt => SeqSSparkConfImpl(bt))

    implicit lazy val sparkConfElement: Elem[SparkConf] = new SeqBaseElemEx[SparkConf, SSparkConf](element[SSparkConf])(weakTypeTag[SparkConf], DefaultOfSparkConf)

  case class SeqSSparkConfImpl
      (override val wrappedValueOfBaseType: Rep[SparkConf])

    extends SSparkConfImpl(wrappedValueOfBaseType)
        with UserTypeSeq[SSparkConfImpl] {
    lazy val selfType = element[SSparkConfImpl]
    override def setAppName(name: Rep[String]): Rep[SSparkConf] =
      SSparkConfImpl(wrappedValueOfBaseType.setAppName(name))

    override def setMaster(master: Rep[String]): Rep[SSparkConf] =
      SSparkConfImpl(wrappedValueOfBaseType.setMaster(master))

    override def set(key: Rep[String], value: Rep[String]): Rep[SSparkConf] =
      SSparkConfImpl(wrappedValueOfBaseType.set(key, value))
  }
  lazy val SSparkConfImpl = new SSparkConfImplCompanionAbs with UserTypeSeq[SSparkConfImplCompanionAbs] {
    lazy val selfType = element[SSparkConfImplCompanionAbs]
  }

  def mkSSparkConfImpl
      (wrappedValueOfBaseType: Rep[SparkConf]): Rep[SSparkConfImpl] =
      new SeqSSparkConfImpl(wrappedValueOfBaseType)
  def unmkSSparkConfImpl(p: Rep[SSparkConf]) = p match {
    case p: SSparkConfImpl @unchecked =>
      Some((p.wrappedValueOfBaseType))
    case _ => None
  }

  implicit def wrapSparkConfToSSparkConf(v: SparkConf): SSparkConf = SSparkConfImpl(v)
}

// Exp -----------------------------------
trait SparkConfsExp extends SparkConfsDsl with ScalanCommunityDslExp {
  self: SparkDslExp =>
  lazy val SSparkConf: Rep[SSparkConfCompanionAbs] = new SSparkConfCompanionAbs with UserTypeDef[SSparkConfCompanionAbs] {
    lazy val selfType = element[SSparkConfCompanionAbs]
    override def mirror(t: Transformer) = this

    def apply: Rep[SSparkConf] =
      newObjEx(classOf[SSparkConf], List())
  }

  implicit lazy val sparkConfElement: Elem[SparkConf] = new ExpBaseElemEx[SparkConf, SSparkConf](element[SSparkConf])(weakTypeTag[SparkConf], DefaultOfSparkConf)

  case class ExpSSparkConfImpl
      (override val wrappedValueOfBaseType: Rep[SparkConf])

    extends SSparkConfImpl(wrappedValueOfBaseType) with UserTypeDef[SSparkConfImpl] {
    lazy val selfType = element[SSparkConfImpl]
    override def mirror(t: Transformer) = ExpSSparkConfImpl(t(wrappedValueOfBaseType))
  }

  lazy val SSparkConfImpl: Rep[SSparkConfImplCompanionAbs] = new SSparkConfImplCompanionAbs with UserTypeDef[SSparkConfImplCompanionAbs] {
    lazy val selfType = element[SSparkConfImplCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object SSparkConfImplMethods {
  }

  def mkSSparkConfImpl
    (wrappedValueOfBaseType: Rep[SparkConf]): Rep[SSparkConfImpl] =
    new ExpSSparkConfImpl(wrappedValueOfBaseType)
  def unmkSSparkConfImpl(p: Rep[SSparkConf]) = p.elem.asInstanceOf[Elem[_]] match {
    case _: SSparkConfImplElem @unchecked =>
      Some((p.asRep[SSparkConfImpl].wrappedValueOfBaseType))
    case _ =>
      None
  }

  object SSparkConfMethods {
    object wrappedValueOfBaseType {
      def unapply(d: Def[_]): Option[Rep[SSparkConf]] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SSparkConfElem[_]] && method.getName == "wrappedValueOfBaseType" =>
          Some(receiver).asInstanceOf[Option[Rep[SSparkConf]]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SSparkConf]] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object setAppName {
      def unapply(d: Def[_]): Option[(Rep[SSparkConf], Rep[String])] = d match {
        case MethodCall(receiver, method, Seq(name, _*), _) if receiver.elem.isInstanceOf[SSparkConfElem[_]] && method.getName == "setAppName" =>
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
        case MethodCall(receiver, method, Seq(master, _*), _) if receiver.elem.isInstanceOf[SSparkConfElem[_]] && method.getName == "setMaster" =>
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
        case MethodCall(receiver, method, Seq(key, value, _*), _) if receiver.elem.isInstanceOf[SSparkConfElem[_]] && method.getName == "set" =>
          Some((receiver, key, value)).asInstanceOf[Option[(Rep[SSparkConf], Rep[String], Rep[String])]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SSparkConf], Rep[String], Rep[String])] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object SSparkConfCompanionMethods {
    object apply {
      def unapply(d: Def[_]): Option[Unit] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem == SSparkConfCompanionElem && method.getName == "apply" =>
          Some(()).asInstanceOf[Option[Unit]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Unit] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }
}

object SparkConfs_Module {
  val packageName = "scalan.spark"
  val name = "SparkConfs"
  val dump = "H4sIAAAAAAAAALVVv28TMRR+uZam+SF+SpXKUqhSEAiSiqVDB9SmKUIKTcUhQAEhORcnHPh87tktFwYGRtgQK0LsbCz8A0iIgQkBEjNTgQHxYwLx7LtLk4oTXchgne3n9773fZ+dp59hlwzgiHQII7zsUUXKtvlekKpk17hyVe+c315ndIl27k48d87xRWnBniaMXSdySbIm5KKPWij63zZdq0OOcIdK5QdSweG6qVBxfMaoo1yfV1zPW1ekxWil7ko1X4fRlt/urcEdyNRhr+NzJ6CK2lVGpKQyXh+nGpHbn+fMvNcQWzV4RXdRGejiQkBchfCxxt4o/jwVdo/7vOcp2B1DawgNC2Oyrif8QCUlspjuut9OpqOc4ALsr98gG6SCJboVWwUu7+LJgiDOTdKlKxiiw0cRsKSsc6EnzHykDnlJ15Cgs55gZiUUAIAKnDIgylv8lPv8lDU/JZsGLmHubaI3VwM/7EH0y4wAhAJTnPhHiiQDrfF26d5V58pPu+BZ+nCooWRNh2OYaCrFDUYK5PHl+Qfy65kncxbkm5B35UJLqoA4alDymK0C4dxXBnOfQBJ0Ua3pNLVMlQWM2WaJnON7gnDMFFNZRJ2Y67hKB+u1YqxOCvVZJWgSmglFpt/voZR+jW+qhLHVzcmTM59qly2whkvkMKWNxg+SpAryti1IcLPq845hVQ+5mOD0Uv2mj25+ab+YhatWn6o4887UwRS75Pu3hTfHTlsw3jReXmak20S2ZI1RrxEgMtWEcX+DBtFOdoMw/fVXtbJt2iHrTMUcDjY/gs0rOJR66wTVzMwbh2cSAgqRSVd8TkvLq6Uf9quHT7UHAyhGO9E1/O3O/fqwu6OMPRVM3AqIELR9kbB12ugsEkm1sAbkHgUjeJ9jfuKVXJoIsRR6OGiC95s5PgNbuiWX8+DA2X/Snzwy35uz1rfJd48tyCHLLVd5RJRmd3g1/qPdYZiggo68ZFiNEI3pYSrZTnfxAIFaz3ykmu17dN/0V/fak/vKWDcTDr+RjdYNfJTmzeFJU6W0DVGxFlaTlmeHt3aGxkiGZjmwFV0d5DCSU+hx33Z59TgzvJjVdZNE+GAVY+GlXjPITmD/0yl2sGMx0BF3fj5aOf762UfzeuS1rGh+rob+UIyG4TYTj5vy+G8xAFTBqNbZQP0D8It7Qb0HAAA="
}
}

