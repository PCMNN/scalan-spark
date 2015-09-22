package scalan.spark

import scalan._
import scalan.common.Default
import org.apache.spark.{HashPartitioner, Partitioner}
import scala.reflect._
import scala.reflect.runtime.universe._

package impl {

import scalan.meta.ScalanAst.STraitOrClassDef

// Abs -----------------------------------
trait PartitionersAbs extends Partitioners with scalan.Scalan {
  self: SparkDsl =>

  // single proxy for each type family
  implicit def proxySPartitioner(p: Rep[SPartitioner]): SPartitioner = {
    proxyOps[SPartitioner](p)(scala.reflect.classTag[SPartitioner])
  }

  // TypeWrapper proxy
  //implicit def proxyPartitioner(p: Rep[Partitioner]): SPartitioner =
  //  proxyOps[SPartitioner](p.asRep[SPartitioner])

  implicit def unwrapValueOfSPartitioner(w: Rep[SPartitioner]): Rep[Partitioner] = w.wrappedValueOfBaseType

  implicit def partitionerElement: Elem[Partitioner]

  // familyElem
  abstract class SPartitionerElem[To <: SPartitioner]
    extends WrapperElem[Partitioner, To] {
    lazy val parent: Option[Elem[_]] = None
    lazy val entityDef: STraitOrClassDef = {
      val module = getModules("Partitioners")
      module.entities.find(_.name == "SPartitioner").get
    }
    lazy val tyArgSubst: Map[String, TypeDesc] = {
      Map()
    }
    override def isEntityType = true
    override lazy val tag = {
      weakTypeTag[SPartitioner].asInstanceOf[WeakTypeTag[To]]
    }
    override def convert(x: Rep[Reifiable[_]]) = {
      implicit val eTo: Elem[To] = this
      val conv = fun {x: Rep[SPartitioner] => convertSPartitioner(x) }
      tryConvert(element[SPartitioner], this, x, conv)
    }

    def convertSPartitioner(x : Rep[SPartitioner]): Rep[To] = {
      assert(x.selfType1 match { case _: SPartitionerElem[_] => true; case _ => false })
      x.asRep[To]
    }
    override def getDefaultRep: Rep[To] = ???
  }

  implicit def sPartitionerElement: Elem[SPartitioner] =
    new SPartitionerElem[SPartitioner] {
      lazy val eTo = element[SPartitionerImpl]
    }

  implicit case object SPartitionerCompanionElem extends CompanionElem[SPartitionerCompanionAbs] {
    lazy val tag = weakTypeTag[SPartitionerCompanionAbs]
    protected def getDefaultRep = SPartitioner
  }

  abstract class SPartitionerCompanionAbs extends CompanionBase[SPartitionerCompanionAbs] with SPartitionerCompanion {
    override def toString = "SPartitioner"
  }
  def SPartitioner: Rep[SPartitionerCompanionAbs]
  implicit def proxySPartitionerCompanion(p: Rep[SPartitionerCompanion]): SPartitionerCompanion =
    proxyOps[SPartitionerCompanion](p)

  // default wrapper implementation
  abstract class SPartitionerImpl(val wrappedValueOfBaseType: Rep[Partitioner]) extends SPartitioner {
  }
  trait SPartitionerImplCompanion
  // elem for concrete class
  class SPartitionerImplElem(val iso: Iso[SPartitionerImplData, SPartitionerImpl])
    extends SPartitionerElem[SPartitionerImpl]
    with ConcreteElem[SPartitionerImplData, SPartitionerImpl] {
    override lazy val parent: Option[Elem[_]] = Some(sPartitionerElement)
    override lazy val entityDef = {
      val module = getModules("Partitioners")
      module.concreteSClasses.find(_.name == "SPartitionerImpl").get
    }
    override lazy val tyArgSubst: Map[String, TypeDesc] = {
      Map()
    }
    lazy val eTo = this
    override def convertSPartitioner(x: Rep[SPartitioner]) = SPartitionerImpl(x.wrappedValueOfBaseType)
    override def getDefaultRep = super[ConcreteElem].getDefaultRep
    override lazy val tag = {
      weakTypeTag[SPartitionerImpl]
    }
  }

  // state representation type
  type SPartitionerImplData = Partitioner

  // 3) Iso for concrete class
  class SPartitionerImplIso
    extends Iso[SPartitionerImplData, SPartitionerImpl] {
    override def from(p: Rep[SPartitionerImpl]) =
      p.wrappedValueOfBaseType
    override def to(p: Rep[Partitioner]) = {
      val wrappedValueOfBaseType = p
      SPartitionerImpl(wrappedValueOfBaseType)
    }
    lazy val defaultRepTo: Rep[SPartitionerImpl] = SPartitionerImpl(DefaultOfPartitioner.value)
    lazy val eTo = new SPartitionerImplElem(this)
  }
  // 4) constructor and deconstructor
  abstract class SPartitionerImplCompanionAbs extends CompanionBase[SPartitionerImplCompanionAbs] {
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

  implicit case object SPartitionerImplCompanionElem extends CompanionElem[SPartitionerImplCompanionAbs] {
    lazy val tag = weakTypeTag[SPartitionerImplCompanionAbs]
    protected def getDefaultRep = SPartitionerImpl
  }

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

  registerModule(scalan.meta.ScalanCodegen.loadModule(Partitioners_Module.dump))
}

// Seq -----------------------------------
trait PartitionersSeq extends PartitionersDsl with scalan.ScalanSeq {
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
trait PartitionersExp extends PartitionersDsl with scalan.ScalanExp {
  self: SparkDslExp =>
  lazy val SPartitioner: Rep[SPartitionerCompanionAbs] = new SPartitionerCompanionAbs with UserTypeDef[SPartitionerCompanionAbs] {
    lazy val selfType = element[SPartitionerCompanionAbs]
    override def mirror(t: Transformer) = this

    def defaultPartitioner(numPartitions: Rep[Int]): Rep[SPartitioner] =
      methodCallEx[SPartitioner](self,
        this.getClass.getMethod("defaultPartitioner", classOf[AnyRef]),
        List(numPartitions.asInstanceOf[AnyRef]))
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
        case MethodCall(receiver, method, Seq(numPartitions, _*), _) if receiver.elem == SPartitionerCompanionElem && method.getName == "defaultPartitioner" =>
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

object Partitioners_Module {
  val packageName = "scalan.spark"
  val name = "Partitioners"
  val dump = "H4sIAAAAAAAAALVVv28TMRR+uZam+SFoK4HULoUqgEAlqVg6dEClBIQUmopDgAJCci5OeuDzubZbEgYGRtgQK0LsbCz8A0iIgQkBEjMTPwbEjwnEs+8uvVYEWMhgne3n7733fZ+dRx9hh5JwQHmEEV4OqCZl134vKl1yq1z7uncmbK0zeoK2b+154p3hx5UDuxowskrUCcUakIs+ql3R/3bpWg1yhHtU6VAqDftqNkPFCxmjnvZDXvGDYF2TJqOVmq/0Qg2Gm2GrtwY3IVODMS/knqSaukuMKEVVvD5KTUV+f56z815dbObgFdNFJdXFOUl8jeVjjrEo/iwVbo+HvBdo2BmXVhemLIzJ+oEIpU5SZBFuNWwl02FOcAEmalfJBqlgik7F1dLnHTxZEMS7Rjp0GUNM+DAWrChrn+sJOx+qQV7RNSTodCCYXekKAEAFjtoiypv8lPv8lA0/JZdKnzD/BjGbKzLs9iD6ZYYAugIhZv8CkSDQKm+Vbl/2Ln13C4FjDndNKVnb4QgCTQ9wg5UCeXx29q76fOrhvAP5BuR9tdhUWhJPpyWP2SoQzkNta+4TSGQH1ZoZpJbNsogx2yyR88JAEI5IMZVF1In5nq9NsFkrxuoMoD6rBU1CM12R6fe7d0C/1jdLhLGV95NH9n+oXnTA2Zoih5AuGl8moBqK7gqRxqMhp9LyaoZcTPHgZP22D77/1Ho6B5edPlkx9r/pgxA71JtXhZeHjjkw2rBuPslIp4F8qSqjQV0uhVw3YDTcoDLayW4QZr5+q1e2RdtknemYxXT7Q9i+hr0D752ghpsF6/FMQkAhsuky8lM6uVL65j6/98i4UEIx2oku4k9//sfbnW1tDaphz3VJhKCt84St03r7OFHUSGuL3KVhCG90zE+8UhgsQyyGGaZs+ISdaxhLa5dc0anU6b9KkDw1XxtzzpfJ1w8cyCHTTV8HRJTm/vGC/EfTwzaSTOQFy2xU0YgZppPtP3k5RaJRNR9p54YBHZ/57F95eEdbA2e6W9/KevMqPk4L9vCkzVOCTaBNXVDy3ensS2keIkmEGce3S2TG/VsXUdliCgofn2IsnxJEXrNVzGIPMwNEdWNKUdeb3+8vH37x+J19CfJGHETkesufg1Wiu82Oo65JhS9/qlQNw0YtW+wvFlb3zIkHAAA="
}
}

