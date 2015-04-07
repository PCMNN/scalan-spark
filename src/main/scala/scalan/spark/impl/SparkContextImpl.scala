package scalan.spark
package impl

import scala.collection.Seq
import org.apache.spark.rdd.RDD
import scalan._
import scalan.common.Default
import org.apache.spark.{SparkConf, SparkContext}
import scala.reflect._
import scala.reflect.runtime.universe._
import scalan.common.Default

// Abs -----------------------------------
trait SparkContextsAbs extends ScalanCommunityDsl with SparkContexts {
  self: SparkDsl =>
  // single proxy for each type family
  implicit def proxySSparkContext(p: Rep[SSparkContext]): SSparkContext = {
    proxyOps[SSparkContext](p)(classTag[SSparkContext])
  }
  // BaseTypeEx proxy
  //implicit def proxySparkContext(p: Rep[SparkContext]): SSparkContext =
  //  proxyOps[SSparkContext](p.asRep[SSparkContext])

  implicit def unwrapValueOfSSparkContext(w: Rep[SSparkContext]): Rep[SparkContext] = w.wrappedValueOfBaseType

  implicit def defaultSSparkContextElem: Elem[SSparkContext] = element[SSparkContextImpl].asElem[SSparkContext]
  implicit def sparkContextElement: Elem[SparkContext]

  class SSparkContextElem[To <: SSparkContext]
    extends EntityElem[To] {
    override def isEntityType = true
    override def tag = {
      weakTypeTag[SSparkContext].asInstanceOf[WeakTypeTag[To]]
    }
    override def convert(x: Rep[Reifiable[_]]) = convertSSparkContext(x.asRep[SSparkContext])
    def convertSSparkContext(x : Rep[SSparkContext]): Rep[To] = {
      assert(x.selfType1.isInstanceOf[SSparkContextElem[_]])
      x.asRep[To]
    }
    override def getDefaultRep: Rep[To] = ???
  }

  implicit def sSparkContextElement =
    new SSparkContextElem[SSparkContext]()

  trait SSparkContextCompanionElem extends CompanionElem[SSparkContextCompanionAbs]
  implicit lazy val SSparkContextCompanionElem: SSparkContextCompanionElem = new SSparkContextCompanionElem {
    lazy val tag = weakTypeTag[SSparkContextCompanionAbs]
    protected def getDefaultRep = SSparkContext
  }

  abstract class SSparkContextCompanionAbs extends CompanionBase[SSparkContextCompanionAbs] with SSparkContextCompanion {
    override def toString = "SSparkContext"

    def apply(conf: Rep[SSparkConf]): Rep[SSparkContext] =
      newObjEx(classOf[SSparkContext], List(conf.asRep[Any]))
  }
  def SSparkContext: Rep[SSparkContextCompanionAbs]
  implicit def proxySSparkContextCompanion(p: Rep[SSparkContextCompanion]): SSparkContextCompanion = {
    proxyOps[SSparkContextCompanion](p)
  }

  // default wrapper implementation
  abstract class SSparkContextImpl(val wrappedValueOfBaseType: Rep[SparkContext]) extends SSparkContext {
    def defaultParallelism: Rep[Int] =
      methodCallEx[Int](self,
        this.getClass.getMethod("defaultParallelism"),
        List())

    def broadcast[T:Elem](value: Rep[T]): Rep[SBroadcast[T]] =
      methodCallEx[SBroadcast[T]](self,
        this.getClass.getMethod("broadcast", classOf[AnyRef], classOf[Elem[T]]),
        List(value.asInstanceOf[AnyRef], element[T]))

    def makeRDD[T:Elem](seq: Rep[SSeq[T]], numSlices: Rep[Int]): Rep[SRDD[T]] =
      methodCallEx[SRDD[T]](self,
        this.getClass.getMethod("makeRDD", classOf[AnyRef], classOf[AnyRef], classOf[Elem[T]]),
        List(seq.asInstanceOf[AnyRef], numSlices.asInstanceOf[AnyRef], element[T]))

    def emptyRDD[T:Elem]: Rep[SRDD[T]] =
      methodCallEx[SRDD[T]](self,
        this.getClass.getMethod("emptyRDD", classOf[Elem[T]]),
        List(element[T]))
  }
  trait SSparkContextImplCompanion
  // elem for concrete class
  class SSparkContextImplElem(val iso: Iso[SSparkContextImplData, SSparkContextImpl])
    extends SSparkContextElem[SSparkContextImpl]
    with ViewElem[SSparkContextImplData, SSparkContextImpl] {
    override def convertSSparkContext(x: Rep[SSparkContext]) = SSparkContextImpl(x.wrappedValueOfBaseType)
    override def getDefaultRep = super[ViewElem].getDefaultRep
    override lazy val tag = super[ViewElem].tag
  }

  // state representation type
  type SSparkContextImplData = SparkContext

  // 3) Iso for concrete class
  class SSparkContextImplIso
    extends Iso[SSparkContextImplData, SSparkContextImpl] {
    override def from(p: Rep[SSparkContextImpl]) =
      unmkSSparkContextImpl(p) match {
        case Some((wrappedValueOfBaseType)) => wrappedValueOfBaseType
        case None => !!!
      }
    override def to(p: Rep[SparkContext]) = {
      val wrappedValueOfBaseType = p
      SSparkContextImpl(wrappedValueOfBaseType)
    }
    lazy val tag = {
      weakTypeTag[SSparkContextImpl]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[SSparkContextImpl]](SSparkContextImpl(DefaultOfSparkContext.value))
    lazy val eTo = new SSparkContextImplElem(this)
  }
  // 4) constructor and deconstructor
  abstract class SSparkContextImplCompanionAbs extends CompanionBase[SSparkContextImplCompanionAbs] with SSparkContextImplCompanion {
    override def toString = "SSparkContextImpl"

    def apply(wrappedValueOfBaseType: Rep[SparkContext]): Rep[SSparkContextImpl] =
      mkSSparkContextImpl(wrappedValueOfBaseType)
    def unapply(p: Rep[SSparkContextImpl]) = unmkSSparkContextImpl(p)
  }
  def SSparkContextImpl: Rep[SSparkContextImplCompanionAbs]
  implicit def proxySSparkContextImplCompanion(p: Rep[SSparkContextImplCompanionAbs]): SSparkContextImplCompanionAbs = {
    proxyOps[SSparkContextImplCompanionAbs](p)
  }

  class SSparkContextImplCompanionElem extends CompanionElem[SSparkContextImplCompanionAbs] {
    lazy val tag = weakTypeTag[SSparkContextImplCompanionAbs]
    protected def getDefaultRep = SSparkContextImpl
  }
  implicit lazy val SSparkContextImplCompanionElem: SSparkContextImplCompanionElem = new SSparkContextImplCompanionElem

  implicit def proxySSparkContextImpl(p: Rep[SSparkContextImpl]): SSparkContextImpl =
    proxyOps[SSparkContextImpl](p)

  implicit class ExtendedSSparkContextImpl(p: Rep[SSparkContextImpl]) {
    def toData: Rep[SSparkContextImplData] = isoSSparkContextImpl.from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoSSparkContextImpl: Iso[SSparkContextImplData, SSparkContextImpl] =
    new SSparkContextImplIso

  // 6) smart constructor and deconstructor
  def mkSSparkContextImpl(wrappedValueOfBaseType: Rep[SparkContext]): Rep[SSparkContextImpl]
  def unmkSSparkContextImpl(p: Rep[SSparkContextImpl]): Option[(Rep[SparkContext])]
}

// Seq -----------------------------------
trait SparkContextsSeq extends SparkContextsDsl with ScalanCommunityDslSeq {
  self: SparkDslSeq =>
  lazy val SSparkContext: Rep[SSparkContextCompanionAbs] = new SSparkContextCompanionAbs with UserTypeSeq[SSparkContextCompanionAbs, SSparkContextCompanionAbs] {
    lazy val selfType = element[SSparkContextCompanionAbs]

    override def apply(conf: Rep[SSparkConf]): Rep[SSparkContext] =
      SSparkContextImpl(new SparkContext(conf))
  }

    // override proxy if we deal with BaseTypeEx
  //override def proxySparkContext(p: Rep[SparkContext]): SSparkContext =
  //  proxyOpsEx[SparkContext,SSparkContext, SeqSSparkContextImpl](p, bt => SeqSSparkContextImpl(bt))

    implicit lazy val sparkContextElement: Elem[SparkContext] = new SeqBaseElemEx[SparkContext, SSparkContext](element[SSparkContext])(weakTypeTag[SparkContext], DefaultOfSparkContext)

  case class SeqSSparkContextImpl
      (override val wrappedValueOfBaseType: Rep[SparkContext])

    extends SSparkContextImpl(wrappedValueOfBaseType)
        with UserTypeSeq[SSparkContext, SSparkContextImpl] {
    lazy val selfType = element[SSparkContextImpl].asInstanceOf[Elem[SSparkContext]]
    override def defaultParallelism: Rep[Int] =
      wrappedValueOfBaseType.defaultParallelism

    override def broadcast[T:Elem](value: Rep[T]): Rep[SBroadcast[T]] =
      wrappedValueOfBaseType.broadcast[T](value)

    override def makeRDD[T:Elem](seq: Rep[SSeq[T]], numSlices: Rep[Int]): Rep[SRDD[T]] =
      wrappedValueOfBaseType.makeRDD[T](seq, numSlices)

    override def emptyRDD[T:Elem]: Rep[SRDD[T]] =
      wrappedValueOfBaseType.emptyRDD[T]
  }
  lazy val SSparkContextImpl = new SSparkContextImplCompanionAbs with UserTypeSeq[SSparkContextImplCompanionAbs, SSparkContextImplCompanionAbs] {
    lazy val selfType = element[SSparkContextImplCompanionAbs]
  }

  def mkSSparkContextImpl
      (wrappedValueOfBaseType: Rep[SparkContext]): Rep[SSparkContextImpl] =
      new SeqSSparkContextImpl(wrappedValueOfBaseType)
  def unmkSSparkContextImpl(p: Rep[SSparkContextImpl]) =
    Some((p.wrappedValueOfBaseType))

  implicit def wrapSparkContextToSSparkContext(v: SparkContext): SSparkContext = SSparkContextImpl(v)
}

// Exp -----------------------------------
trait SparkContextsExp extends SparkContextsDsl with ScalanCommunityDslExp {
  self: SparkDslExp =>
  lazy val SSparkContext: Rep[SSparkContextCompanionAbs] = new SSparkContextCompanionAbs with UserTypeDef[SSparkContextCompanionAbs, SSparkContextCompanionAbs] {
    lazy val selfType = element[SSparkContextCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  implicit lazy val sparkContextElement: Elem[SparkContext] = new ExpBaseElemEx[SparkContext, SSparkContext](element[SSparkContext])(weakTypeTag[SparkContext], DefaultOfSparkContext)
  case class ExpSSparkContextImpl
      (override val wrappedValueOfBaseType: Rep[SparkContext])

    extends SSparkContextImpl(wrappedValueOfBaseType) with UserTypeDef[SSparkContext, SSparkContextImpl] {
    lazy val selfType = element[SSparkContextImpl].asInstanceOf[Elem[SSparkContext]]
    override def mirror(t: Transformer) = ExpSSparkContextImpl(t(wrappedValueOfBaseType))
  }

  lazy val SSparkContextImpl: Rep[SSparkContextImplCompanionAbs] = new SSparkContextImplCompanionAbs with UserTypeDef[SSparkContextImplCompanionAbs, SSparkContextImplCompanionAbs] {
    lazy val selfType = element[SSparkContextImplCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object SSparkContextImplMethods {
  }

  def mkSSparkContextImpl
    (wrappedValueOfBaseType: Rep[SparkContext]): Rep[SSparkContextImpl] =
    new ExpSSparkContextImpl(wrappedValueOfBaseType)
  def unmkSSparkContextImpl(p: Rep[SSparkContextImpl]) =
    Some((p.wrappedValueOfBaseType))

  object SSparkContextMethods {
    object wrappedValueOfBaseType {
      def unapply(d: Def[_]): Option[Rep[SSparkContext]] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SSparkContextElem[_]] && method.getName == "wrappedValueOfBaseType" =>
          Some(receiver).asInstanceOf[Option[Rep[SSparkContext]]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SSparkContext]] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object defaultParallelism {
      def unapply(d: Def[_]): Option[Rep[SSparkContext]] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SSparkContextElem[_]] && method.getName == "defaultParallelism" =>
          Some(receiver).asInstanceOf[Option[Rep[SSparkContext]]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SSparkContext]] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object broadcast {
      def unapply(d: Def[_]): Option[(Rep[SSparkContext], Rep[T]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(value, _*), _) if receiver.elem.isInstanceOf[SSparkContextElem[_]] && method.getName == "broadcast" =>
          Some((receiver, value)).asInstanceOf[Option[(Rep[SSparkContext], Rep[T]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SSparkContext], Rep[T]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object makeRDD {
      def unapply(d: Def[_]): Option[(Rep[SSparkContext], Rep[SSeq[T]], Rep[Int]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(seq, numSlices, _*), _) if receiver.elem.isInstanceOf[SSparkContextElem[_]] && method.getName == "makeRDD" =>
          Some((receiver, seq, numSlices)).asInstanceOf[Option[(Rep[SSparkContext], Rep[SSeq[T]], Rep[Int]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SSparkContext], Rep[SSeq[T]], Rep[Int]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object emptyRDD {
      def unapply(d: Def[_]): Option[Rep[SSparkContext] forSome {type T}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SSparkContextElem[_]] && method.getName == "emptyRDD" =>
          Some(receiver).asInstanceOf[Option[Rep[SSparkContext] forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SSparkContext] forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object SSparkContextCompanionMethods {
    object apply {
      def unapply(d: Def[_]): Option[Rep[SSparkConf]] = d match {
        case MethodCall(receiver, method, Seq(conf, _*), _) if receiver.elem.isInstanceOf[SSparkContextCompanionElem] && method.getName == "apply" =>
          Some(conf).asInstanceOf[Option[Rep[SSparkConf]]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SSparkConf]] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }
}
