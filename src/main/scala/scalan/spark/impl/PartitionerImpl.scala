package scalan.spark
package impl

import scalan._
import scalan.common.Default
import org.apache.spark.{HashPartitioner, Partitioner}
import scala.reflect._
import scala.reflect.runtime.universe._
import scalan.common.Default

// Abs -----------------------------------
trait PartitionersAbs extends Partitioners with ScalanCommunityDsl {
  self: SparkDsl =>

  // single proxy for each type family
  implicit def proxySPartitioner(p: Rep[SPartitioner]): SPartitioner = {
    proxyOps[SPartitioner](p)(classTag[SPartitioner])
  }

  // TypeWrapper proxy
  //implicit def proxyPartitioner(p: Rep[Partitioner]): SPartitioner =
  //  proxyOps[SPartitioner](p.asRep[SPartitioner])

  implicit def unwrapValueOfSPartitioner(w: Rep[SPartitioner]): Rep[Partitioner] = w.wrappedValueOfBaseType

  implicit def partitionerElement: Elem[Partitioner]

  // familyElem
  abstract class SPartitionerElem[To <: SPartitioner]
    extends WrapperElem[Partitioner, To] {
    override def isEntityType = true
    override def tag = {
      weakTypeTag[SPartitioner].asInstanceOf[WeakTypeTag[To]]
    }
    override def convert(x: Rep[Reifiable[_]]) = {
      val conv = fun {x: Rep[SPartitioner] =>  convertSPartitioner(x) }
      tryConvert(element[SPartitioner], this, x, conv)
    }

    def convertSPartitioner(x : Rep[SPartitioner]): Rep[To] = {
      assert(x.selfType1 match { case _: SPartitionerElem[_] => true case _ => false })
      x.asRep[To]
    }
    override def getDefaultRep: Rep[To] = ???
  }

  implicit def sPartitionerElement: Elem[SPartitioner] =
    new SPartitionerElem[SPartitioner] {
      lazy val eTo = element[SPartitionerImpl]
    }

  trait SPartitionerCompanionElem extends CompanionElem[SPartitionerCompanionAbs]
  implicit lazy val SPartitionerCompanionElem: SPartitionerCompanionElem = new SPartitionerCompanionElem {
    lazy val tag = weakTypeTag[SPartitionerCompanionAbs]
    protected def getDefaultRep = SPartitioner
  }

  abstract class SPartitionerCompanionAbs extends CompanionBase[SPartitionerCompanionAbs] with SPartitionerCompanion {
    override def toString = "SPartitioner"

    def defaultPartitioner(numPartitions: Rep[Int]): Rep[SPartitioner] =
      methodCallEx[SPartitioner](self,
        this.getClass.getMethod("defaultPartitioner", classOf[AnyRef]),
        List(numPartitions.asInstanceOf[AnyRef]))
  }
  def SPartitioner: Rep[SPartitionerCompanionAbs]
  implicit def proxySPartitionerCompanion(p: Rep[SPartitionerCompanion]): SPartitionerCompanion = {
    proxyOps[SPartitionerCompanion](p)
  }

  // default wrapper implementation
  abstract class SPartitionerImpl(val wrappedValueOfBaseType: Rep[Partitioner]) extends SPartitioner {
  }
  trait SPartitionerImplCompanion
  // elem for concrete class
  class SPartitionerImplElem(val iso: Iso[SPartitionerImplData, SPartitionerImpl])
    extends SPartitionerElem[SPartitionerImpl]
    with ConcreteElem[SPartitionerImplData, SPartitionerImpl] {
    lazy val eTo = this
    override def convertSPartitioner(x: Rep[SPartitioner]) = SPartitionerImpl(x.wrappedValueOfBaseType)
    override def getDefaultRep = super[ConcreteElem].getDefaultRep
    override lazy val tag = super[ConcreteElem].tag
  }

  // state representation type
  type SPartitionerImplData = Partitioner

  // 3) Iso for concrete class
  class SPartitionerImplIso
    extends Iso[SPartitionerImplData, SPartitionerImpl]()((implicitly[Elem[Partitioner]])) {
    override def from(p: Rep[SPartitionerImpl]) =
      p.wrappedValueOfBaseType
    override def to(p: Rep[Partitioner]) = {
      val wrappedValueOfBaseType = p
      SPartitionerImpl(wrappedValueOfBaseType)
    }
    lazy val tag = {
      weakTypeTag[SPartitionerImpl]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[SPartitionerImpl]](SPartitionerImpl(DefaultOfPartitioner.value))
    lazy val eTo = new SPartitionerImplElem(this)
  }
  // 4) constructor and deconstructor
  abstract class SPartitionerImplCompanionAbs extends CompanionBase[SPartitionerImplCompanionAbs] with SPartitionerImplCompanion {
    override def toString = "SPartitionerImpl"

    def apply(wrappedValueOfBaseType: Rep[Partitioner]): Rep[SPartitionerImpl] =
      mkSPartitionerImpl(wrappedValueOfBaseType)
  }
  object SPartitionerImplMatcher {
    def unapply(p: Rep[SPartitioner]) = unmkSPartitionerImpl(p)
  }
  def SPartitionerImpl: Rep[SPartitionerImplCompanionAbs]
  implicit def proxySPartitionerImplCompanion(p: Rep[SPartitionerImplCompanionAbs]): SPartitionerImplCompanionAbs = {
    proxyOps[SPartitionerImplCompanionAbs](p)
  }

  class SPartitionerImplCompanionElem extends CompanionElem[SPartitionerImplCompanionAbs] {
    lazy val tag = weakTypeTag[SPartitionerImplCompanionAbs]
    protected def getDefaultRep = SPartitionerImpl
  }
  implicit lazy val SPartitionerImplCompanionElem: SPartitionerImplCompanionElem = new SPartitionerImplCompanionElem

  implicit def proxySPartitionerImpl(p: Rep[SPartitionerImpl]): SPartitionerImpl =
    proxyOps[SPartitionerImpl](p)

  implicit class ExtendedSPartitionerImpl(p: Rep[SPartitionerImpl]) {
    def toData: Rep[SPartitionerImplData] = isoSPartitionerImpl.from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoSPartitionerImpl: Iso[SPartitionerImplData, SPartitionerImpl] =
    new SPartitionerImplIso

  // 6) smart constructor and deconstructor
  def mkSPartitionerImpl(wrappedValueOfBaseType: Rep[Partitioner]): Rep[SPartitionerImpl]
  def unmkSPartitionerImpl(p: Rep[SPartitioner]): Option[(Rep[Partitioner])]
}

// Seq -----------------------------------
trait PartitionersSeq extends PartitionersDsl with ScalanCommunityDslSeq {
  self: SparkDslSeq =>
  lazy val SPartitioner: Rep[SPartitionerCompanionAbs] = new SPartitionerCompanionAbs with UserTypeSeq[SPartitionerCompanionAbs] {
    lazy val selfType = element[SPartitionerCompanionAbs]
    override def defaultPartitioner(numPartitions: Rep[Int]): Rep[SPartitioner] =
      ??? //SPartitionerImpl(Partitioner.defaultPartitioner(numPartitions))
  }

    // override proxy if we deal with TypeWrapper
  //override def proxyPartitioner(p: Rep[Partitioner]): SPartitioner =
  //  proxyOpsEx[Partitioner,SPartitioner, SeqSPartitionerImpl](p, bt => SeqSPartitionerImpl(bt))

    implicit lazy val partitionerElement: Elem[Partitioner] = new SeqBaseElemEx[Partitioner, SPartitioner](element[SPartitioner])(weakTypeTag[Partitioner], DefaultOfPartitioner)

  case class SeqSPartitionerImpl
      (override val wrappedValueOfBaseType: Rep[Partitioner])

    extends SPartitionerImpl(wrappedValueOfBaseType)
        with UserTypeSeq[SPartitionerImpl] {
    lazy val selfType = element[SPartitionerImpl]
  }
  lazy val SPartitionerImpl = new SPartitionerImplCompanionAbs with UserTypeSeq[SPartitionerImplCompanionAbs] {
    lazy val selfType = element[SPartitionerImplCompanionAbs]
  }

  def mkSPartitionerImpl
      (wrappedValueOfBaseType: Rep[Partitioner]): Rep[SPartitionerImpl] =
      new SeqSPartitionerImpl(wrappedValueOfBaseType)
  def unmkSPartitionerImpl(p: Rep[SPartitioner]) = p match {
    case p: SPartitionerImpl @unchecked =>
      Some((p.wrappedValueOfBaseType))
    case _ => None
  }

  implicit def wrapPartitionerToSPartitioner(v: Partitioner): SPartitioner = SPartitionerImpl(v)
}

// Exp -----------------------------------
trait PartitionersExp extends PartitionersDsl with ScalanCommunityDslExp {
  self: SparkDslExp =>
  lazy val SPartitioner: Rep[SPartitionerCompanionAbs] = new SPartitionerCompanionAbs with UserTypeDef[SPartitionerCompanionAbs] {
    lazy val selfType = element[SPartitionerCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  implicit lazy val partitionerElement: Elem[Partitioner] = new ExpBaseElemEx[Partitioner, SPartitioner](element[SPartitioner])(weakTypeTag[Partitioner], DefaultOfPartitioner)

  case class ExpSPartitionerImpl
      (override val wrappedValueOfBaseType: Rep[Partitioner])

    extends SPartitionerImpl(wrappedValueOfBaseType) with UserTypeDef[SPartitionerImpl] {
    lazy val selfType = element[SPartitionerImpl]
    override def mirror(t: Transformer) = ExpSPartitionerImpl(t(wrappedValueOfBaseType))
  }

  lazy val SPartitionerImpl: Rep[SPartitionerImplCompanionAbs] = new SPartitionerImplCompanionAbs with UserTypeDef[SPartitionerImplCompanionAbs] {
    lazy val selfType = element[SPartitionerImplCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object SPartitionerImplMethods {
  }

  def mkSPartitionerImpl
    (wrappedValueOfBaseType: Rep[Partitioner]): Rep[SPartitionerImpl] =
    new ExpSPartitionerImpl(wrappedValueOfBaseType)
  def unmkSPartitionerImpl(p: Rep[SPartitioner]) = p.elem.asInstanceOf[Elem[_]] match {
    case _: SPartitionerImplElem @unchecked =>
      Some((p.asRep[SPartitionerImpl].wrappedValueOfBaseType))
    case _ =>
      None
  }

  object SPartitionerMethods {
    object wrappedValueOfBaseType {
      def unapply(d: Def[_]): Option[Rep[SPartitioner]] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SPartitionerElem[_]] && method.getName == "wrappedValueOfBaseType" =>
          Some(receiver).asInstanceOf[Option[Rep[SPartitioner]]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SPartitioner]] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object SPartitionerCompanionMethods {
    object defaultPartitioner {
      def unapply(d: Def[_]): Option[Rep[Int]] = d match {
        case MethodCall(receiver, method, Seq(numPartitions, _*), _) if receiver.elem.isInstanceOf[SPartitionerCompanionElem] && method.getName == "defaultPartitioner" =>
          Some(numPartitions).asInstanceOf[Option[Rep[Int]]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[Int]] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }
}
