package scalan.spark
package impl

import scalan._
import scalan.common.Default
import org.apache.spark.SparkContext
import scala.reflect.runtime.universe._
import scalan.common.Default

trait SparkContextsAbs extends Scalan with SparkContexts
{ self: SparkDsl =>
  // single proxy for each type family
  implicit def proxySSparkContext(p: Rep[SSparkContext]): SSparkContext =
    proxyOps[SSparkContext](p)
  // BaseTypeEx proxy
  implicit def proxySparkContext(p: Rep[SparkContext]): SSparkContext =
    proxyOps[SSparkContext](p.asRep[SSparkContext])

  implicit def defaultSSparkContextElem: Elem[SSparkContext] = element[SSparkContextImpl].asElem[SSparkContext]
  implicit def SparkContextElement: Elem[SparkContext]

  abstract class SSparkContextElem[From, To <: SSparkContext](iso: Iso[From, To]) extends ViewElem[From, To]()(iso)

  trait SSparkContextCompanionElem extends CompanionElem[SSparkContextCompanionAbs]
  implicit lazy val SSparkContextCompanionElem: SSparkContextCompanionElem = new SSparkContextCompanionElem {
    lazy val tag = typeTag[SSparkContextCompanionAbs]
    lazy val getDefaultRep = Default.defaultVal(SSparkContext)
    //def getDefaultRep = defaultRep
  }

  abstract class SSparkContextCompanionAbs extends CompanionBase[SSparkContextCompanionAbs] with SSparkContextCompanion {
    override def toString = "SSparkContext"
    
  }
  def SSparkContext: Rep[SSparkContextCompanionAbs]
  implicit def proxySSparkContextCompanion(p: Rep[SSparkContextCompanion]): SSparkContextCompanion = {
    proxyOps[SSparkContextCompanion](p)
  }

  //default wrapper implementation
    abstract class SSparkContextImpl(val value: Rep[SparkContext]) extends SSparkContext {
    
    def defaultParallelism: Rep[Int] =
      methodCallEx[Int](self,
        this.getClass.getMethod("defaultParallelism"),
        List())

    
    def broadcast[T:Elem](value: Rep[T]): RepBroadcast[T] =
      methodCallEx[Broadcast[T]](self,
        this.getClass.getMethod("broadcast", classOf[AnyRef]),
        List(value.asInstanceOf[AnyRef]))

    
    def makeRDD[T:Elem](seq: Rep[Seq[T]], numSlices: Rep[Int]): RepRDD[T] =
      methodCallEx[RDD[T]](self,
        this.getClass.getMethod("makeRDD", classOf[AnyRef], classOf[AnyRef]),
        List(seq.asInstanceOf[AnyRef], numSlices.asInstanceOf[AnyRef]))

    
    def emptyRDD[T:Elem]: RepRDD[T] =
      methodCallEx[RDD[T]](self,
        this.getClass.getMethod("emptyRDD"),
        List())

  }
  trait SSparkContextImplCompanion
  // elem for concrete class
  class SSparkContextImplElem(iso: Iso[SSparkContextImplData, SSparkContextImpl]) extends SSparkContextElem[SSparkContextImplData, SSparkContextImpl](iso)

  // state representation type
  type SSparkContextImplData = SparkContext

  // 3) Iso for concrete class
  class SSparkContextImplIso
    extends Iso[SSparkContextImplData, SSparkContextImpl] {
    override def from(p: Rep[SSparkContextImpl]) =
      unmkSSparkContextImpl(p) match {
        case Some((value)) => value
        case None => !!!
      }
    override def to(p: Rep[SparkContext]) = {
      val value = p
      SSparkContextImpl(value)
    }
    lazy val tag = {
      weakTypeTag[SSparkContextImpl]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[SSparkContextImpl]](SSparkContextImpl(Default.defaultOf[SparkContext]))
    lazy val eTo = new SSparkContextImplElem(this)
  }
  // 4) constructor and deconstructor
  abstract class SSparkContextImplCompanionAbs extends CompanionBase[SSparkContextImplCompanionAbs] with SSparkContextImplCompanion {
    override def toString = "SSparkContextImpl"

    def apply(value: Rep[SparkContext]): Rep[SSparkContextImpl] =
      mkSSparkContextImpl(value)
    def unapply(p: Rep[SSparkContextImpl]) = unmkSSparkContextImpl(p)
  }
  def SSparkContextImpl: Rep[SSparkContextImplCompanionAbs]
  implicit def proxySSparkContextImplCompanion(p: Rep[SSparkContextImplCompanionAbs]): SSparkContextImplCompanionAbs = {
    proxyOps[SSparkContextImplCompanionAbs](p)
  }

  class SSparkContextImplCompanionElem extends CompanionElem[SSparkContextImplCompanionAbs] {
    lazy val tag = typeTag[SSparkContextImplCompanionAbs]
    lazy val getDefaultRep = Default.defaultVal(SSparkContextImpl)
    //def getDefaultRep = defaultRep
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
  def mkSSparkContextImpl(value: Rep[SparkContext]): Rep[SSparkContextImpl]
  def unmkSSparkContextImpl(p: Rep[SSparkContextImpl]): Option[(Rep[SparkContext])]
}

trait SparkContextsSeq extends SparkContextsAbs with SparkContextsDsl with ScalanSeq { self: SparkContextsDslSeq =>
  lazy val SSparkContext: Rep[SSparkContextCompanionAbs] = new SSparkContextCompanionAbs with UserTypeSeq[SSparkContextCompanionAbs, SSparkContextCompanionAbs] {
    lazy val selfType = element[SSparkContextCompanionAbs]
    
  }

    // override proxy if we deal with BaseTypeEx
  override def proxySparkContext(p: Rep[SparkContext]): SSparkContext =
    proxyOpsEx[SparkContext,SSparkContext, SeqSSparkContextImpl](p, bt => SeqSSparkContextImpl(bt))

    implicit lazy val SparkContextElement: Elem[SparkContext] = new SeqBaseElemEx[SparkContext, SSparkContext](element[SSparkContext])

  case class SeqSSparkContextImpl
      (override val value: Rep[SparkContext])
      
    extends SSparkContextImpl(value)
        with UserTypeSeq[SSparkContext, SSparkContextImpl] {
    lazy val selfType = element[SSparkContextImpl].asInstanceOf[Elem[SSparkContext]]
    
    override def defaultParallelism: Rep[Int] =
      value.defaultParallelism

    
    override def broadcast[T:Elem](value: Rep[T]): RepBroadcast[T] =
      value.broadcast[T](value)

    
    override def makeRDD[T:Elem](seq: Rep[Seq[T]], numSlices: Rep[Int]): RepRDD[T] =
      value.makeRDD[T](seq, numSlices)

    
    override def emptyRDD[T:Elem]: RepRDD[T] =
      value.emptyRDD[T]

  }
  lazy val SSparkContextImpl = new SSparkContextImplCompanionAbs with UserTypeSeq[SSparkContextImplCompanionAbs, SSparkContextImplCompanionAbs] {
    lazy val selfType = element[SSparkContextImplCompanionAbs]
  }

  def mkSSparkContextImpl
      (value: Rep[SparkContext]) =
      new SeqSSparkContextImpl(value)
  def unmkSSparkContextImpl(p: Rep[SSparkContextImpl]) =
    Some((p.value))
}

trait SparkContextsExp extends SparkContextsAbs with SparkContextsDsl with ScalanExp {
  lazy val SSparkContext: Rep[SSparkContextCompanionAbs] = new SSparkContextCompanionAbs with UserTypeDef[SSparkContextCompanionAbs, SSparkContextCompanionAbs] {
    lazy val selfType = element[SSparkContextCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  implicit lazy val SparkContextElement: Elem[SparkContext] = new ExpBaseElemEx[SparkContext, SSparkContext](element[SSparkContext])

  case class ExpSSparkContextImpl
      (override val value: Rep[SparkContext])
      
    extends SSparkContextImpl(value) with UserTypeDef[SSparkContext, SSparkContextImpl] {
    lazy val selfType = element[SSparkContextImpl].asInstanceOf[Elem[SSparkContext]]
    override def mirror(t: Transformer) = ExpSSparkContextImpl(t(value))
  }

  lazy val SSparkContextImpl: Rep[SSparkContextImplCompanionAbs] = new SSparkContextImplCompanionAbs with UserTypeDef[SSparkContextImplCompanionAbs, SSparkContextImplCompanionAbs] {
    lazy val selfType = element[SSparkContextImplCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object SSparkContextImplMethods {

  }



  def mkSSparkContextImpl
    (value: Rep[SparkContext]) =
    new ExpSSparkContextImpl(value)
  def unmkSSparkContextImpl(p: Rep[SSparkContextImpl]) =
    Some((p.value))

  object SSparkContextMethods {
    object defaultParallelism {
      def unapply(d: Def[_]): Option[Rep[SSparkContext]] = d match {
        case MethodCall(receiver, method, _) if receiver.elem.isInstanceOf[SSparkContextElem[_, _]] && method.getName == "defaultParallelism" =>
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
        case MethodCall(receiver, method, Seq(value, _*)) if receiver.elem.isInstanceOf[SSparkContextElem[_, _]] && method.getName == "broadcast" =>
          Some((receiver, value)).asInstanceOf[Option[(Rep[SSparkContext], Rep[T]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SSparkContext], Rep[T]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object makeRDD {
      def unapply(d: Def[_]): Option[(Rep[SSparkContext], Rep[Seq[T]], Rep[Int]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(seq, numSlices, _*)) if receiver.elem.isInstanceOf[SSparkContextElem[_, _]] && method.getName == "makeRDD" =>
          Some((receiver, seq, numSlices)).asInstanceOf[Option[(Rep[SSparkContext], Rep[Seq[T]], Rep[Int]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SSparkContext], Rep[Seq[T]], Rep[Int]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object emptyRDD {
      def unapply(d: Def[_]): Option[Rep[SSparkContext] forSome {type T}] = d match {
        case MethodCall(receiver, method, _) if receiver.elem.isInstanceOf[SSparkContextElem[_, _]] && method.getName == "emptyRDD" =>
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

  }
}
