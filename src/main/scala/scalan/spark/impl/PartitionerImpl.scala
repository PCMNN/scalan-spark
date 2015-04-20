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

  // familyElem
  class SPartitionerElem[To <: SPartitioner]
    extends EntityElem[To] {
    override def isEntityType = true
    override def tag = {
      weakTypeTag[SPartitioner].asInstanceOf[WeakTypeTag[To]]
    }
    override def convert(x: Rep[Reifiable[_]]) = convertSPartitioner(x.asRep[SPartitioner])
    def convertSPartitioner(x : Rep[SPartitioner]): Rep[To] = {
      //assert(x.selfType1.isInstanceOf[SPartitionerElem[_]])
      x.asRep[To]
    }
    override def getDefaultRep: Rep[To] = ???
  }

  implicit def sPartitionerElement =
    new SPartitionerElem[SPartitioner]()

  trait SPartitionerCompanionElem extends CompanionElem[SPartitionerCompanionAbs]
  implicit lazy val SPartitionerCompanionElem: SPartitionerCompanionElem = new SPartitionerCompanionElem {
    lazy val tag = weakTypeTag[SPartitionerCompanionAbs]
    protected def getDefaultRep = SPartitioner
  }

  abstract class SPartitionerCompanionAbs extends CompanionBase[SPartitionerCompanionAbs] with SPartitionerCompanion {
    override def toString = "SPartitioner"
  }
  def SPartitioner: Rep[SPartitionerCompanionAbs]
  implicit def proxySPartitionerCompanion(p: Rep[SPartitionerCompanion]): SPartitionerCompanion = {
    proxyOps[SPartitionerCompanion](p)
  }

  // single proxy for each type family
  implicit def proxySBasePartitioner(p: Rep[SBasePartitioner]): SBasePartitioner = {
    proxyOps[SBasePartitioner](p)(classTag[SBasePartitioner])
  }
  // familyElem
  class SBasePartitionerElem[To <: SBasePartitioner]
    extends EntityElem[To] {
    override def isEntityType = true
    override def tag = {
      weakTypeTag[SBasePartitioner].asInstanceOf[WeakTypeTag[To]]
    }
    override def convert(x: Rep[Reifiable[_]]) = convertSBasePartitioner(x.asRep[SBasePartitioner])
    def convertSBasePartitioner(x : Rep[SBasePartitioner]): Rep[To] = {
      //assert(x.selfType1.isInstanceOf[SBasePartitionerElem[_]])
      x.asRep[To]
    }
    override def getDefaultRep: Rep[To] = ???
  }

  implicit def sBasePartitionerElement =
    new SBasePartitionerElem[SBasePartitioner]()
}

// Seq -----------------------------------
trait PartitionersSeq extends PartitionersDsl with ScalanCommunityDslSeq {
  self: SparkDslSeq =>
  lazy val SPartitioner: Rep[SPartitionerCompanionAbs] = new SPartitionerCompanionAbs with UserTypeSeq[SPartitionerCompanionAbs] {
    lazy val selfType = element[SPartitionerCompanionAbs]
  }
}

// Exp -----------------------------------
trait PartitionersExp extends PartitionersDsl with ScalanCommunityDslExp {
  self: SparkDslExp =>
  lazy val SPartitioner: Rep[SPartitionerCompanionAbs] = new SPartitionerCompanionAbs with UserTypeDef[SPartitionerCompanionAbs] {
    lazy val selfType = element[SPartitionerCompanionAbs]
    override def mirror(t: Transformer) = this
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
  }
}
