package scalan.spark
package impl

import scala.reflect.ClassTag
import org.apache.spark.rdd._
import scalan._
import scalan.common.Default
import org.apache.spark.Partitioner
import scala.reflect.runtime.universe._
import scalan.common.Default

// Abs -----------------------------------
trait PairRDDFunctionssAbs extends Scalan with PairRDDFunctionss
{ self: SparkDsl =>
  // single proxy for each type family
  implicit def proxySPairRDDFunctions[K, V](p: Rep[SPairRDDFunctions[K, V]]): SPairRDDFunctions[K, V] =
    proxyOps[SPairRDDFunctions[K, V]](p)
  // BaseTypeEx proxy
  implicit def proxyPairRDDFunctions[K:Elem, V:Elem](p: Rep[PairRDDFunctions[K, V]]): SPairRDDFunctions[K, V] =
    proxyOps[SPairRDDFunctions[K, V]](p.asRep[SPairRDDFunctions[K, V]])

  implicit def defaultSPairRDDFunctionsElem[K:Elem, V:Elem]: Elem[SPairRDDFunctions[K, V]] = element[SPairRDDFunctionsImpl[K, V]].asElem[SPairRDDFunctions[K, V]]
  implicit def PairRDDFunctionsElement[K:Elem:WeakTypeTag, V:Elem:WeakTypeTag]: Elem[PairRDDFunctions[K, V]]

  abstract class SPairRDDFunctionsElem[K, V, From, To <: SPairRDDFunctions[K, V]](iso: Iso[From, To]) extends ViewElem[From, To]()(iso)

  trait SPairRDDFunctionsCompanionElem extends CompanionElem[SPairRDDFunctionsCompanionAbs]
  implicit lazy val SPairRDDFunctionsCompanionElem: SPairRDDFunctionsCompanionElem = new SPairRDDFunctionsCompanionElem {
    lazy val tag = typeTag[SPairRDDFunctionsCompanionAbs]
    protected def getDefaultRep = SPairRDDFunctions
  }

  abstract class SPairRDDFunctionsCompanionAbs extends CompanionBase[SPairRDDFunctionsCompanionAbs] with SPairRDDFunctionsCompanion {
    override def toString = "SPairRDDFunctions"
    
    def apply[K:Elem, V:Elem](rdd: Rep[RDD[(K,V)]]): Rep[PairRDDFunctions[K,V]] =
      newObjEx(classOf[PairRDDFunctions[K, V]], scala.collection.immutable.List(rdd.asRep[Any]))

  }
  def SPairRDDFunctions: Rep[SPairRDDFunctionsCompanionAbs]
  implicit def proxySPairRDDFunctionsCompanion(p: Rep[SPairRDDFunctionsCompanion]): SPairRDDFunctionsCompanion = {
    proxyOps[SPairRDDFunctionsCompanion](p)
  }

  //default wrapper implementation
    abstract class SPairRDDFunctionsImpl[K, V](val wrappedValueOfBaseType: Rep[PairRDDFunctions[K, V]])(implicit val eK: Elem[K], val eV: Elem[V]) extends SPairRDDFunctions[K, V] {
    
    def partitionBy(partitioner: Rep[Partitioner]): Rep[RDD[(K,V)]] =
      methodCallEx[RDD[(K,V)]](self,
        this.getClass.getMethod("partitionBy", classOf[AnyRef]),
        scala.collection.immutable.List(partitioner.asInstanceOf[AnyRef]))

    
    def reduceByKey(func: Rep[((V,V)) => V]): Rep[PairRDDFunctions[K,V]] =
      methodCallEx[PairRDDFunctions[K,V]](self,
        this.getClass.getMethod("reduceByKey", classOf[AnyRef]),
        scala.collection.immutable.List(func.asInstanceOf[AnyRef]))

    
    def values: Rep[RDD[V]] =
      methodCallEx[RDD[V]](self,
        this.getClass.getMethod("values"),
        scala.collection.immutable.List())

  }
  trait SPairRDDFunctionsImplCompanion
  // elem for concrete class
  class SPairRDDFunctionsImplElem[K, V](iso: Iso[SPairRDDFunctionsImplData[K, V], SPairRDDFunctionsImpl[K, V]]) extends SPairRDDFunctionsElem[K, V, SPairRDDFunctionsImplData[K, V], SPairRDDFunctionsImpl[K, V]](iso)

  // state representation type
  type SPairRDDFunctionsImplData[K, V] = PairRDDFunctions[K,V]

  // 3) Iso for concrete class
  class SPairRDDFunctionsImplIso[K, V](implicit eK: Elem[K], eV: Elem[V])
    extends Iso[SPairRDDFunctionsImplData[K, V], SPairRDDFunctionsImpl[K, V]] {
    override def from(p: Rep[SPairRDDFunctionsImpl[K, V]]) =
      unmkSPairRDDFunctionsImpl(p) match {
        case Some((wrappedValueOfBaseType)) => wrappedValueOfBaseType
        case None => !!!
      }
    override def to(p: Rep[PairRDDFunctions[K,V]]) = {
      val wrappedValueOfBaseType = p
      SPairRDDFunctionsImpl(wrappedValueOfBaseType)
    }
    lazy val tag = {
      weakTypeTag[SPairRDDFunctionsImpl[K, V]]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[SPairRDDFunctionsImpl[K, V]]](SPairRDDFunctionsImpl(Default.defaultOf[PairRDDFunctions[K,V]]))
    lazy val eTo = new SPairRDDFunctionsImplElem[K, V](this)
  }
  // 4) constructor and deconstructor
  abstract class SPairRDDFunctionsImplCompanionAbs extends CompanionBase[SPairRDDFunctionsImplCompanionAbs] with SPairRDDFunctionsImplCompanion {
    override def toString = "SPairRDDFunctionsImpl"

    def apply[K, V](wrappedValueOfBaseType: Rep[PairRDDFunctions[K,V]])(implicit eK: Elem[K], eV: Elem[V]): Rep[SPairRDDFunctionsImpl[K, V]] =
      mkSPairRDDFunctionsImpl(wrappedValueOfBaseType)
    def unapply[K:Elem, V:Elem](p: Rep[SPairRDDFunctionsImpl[K, V]]) = unmkSPairRDDFunctionsImpl(p)
  }
  def SPairRDDFunctionsImpl: Rep[SPairRDDFunctionsImplCompanionAbs]
  implicit def proxySPairRDDFunctionsImplCompanion(p: Rep[SPairRDDFunctionsImplCompanionAbs]): SPairRDDFunctionsImplCompanionAbs = {
    proxyOps[SPairRDDFunctionsImplCompanionAbs](p)
  }

  class SPairRDDFunctionsImplCompanionElem extends CompanionElem[SPairRDDFunctionsImplCompanionAbs] {
    lazy val tag = typeTag[SPairRDDFunctionsImplCompanionAbs]
    protected def getDefaultRep = SPairRDDFunctionsImpl
  }
  implicit lazy val SPairRDDFunctionsImplCompanionElem: SPairRDDFunctionsImplCompanionElem = new SPairRDDFunctionsImplCompanionElem

  implicit def proxySPairRDDFunctionsImpl[K, V](p: Rep[SPairRDDFunctionsImpl[K, V]]): SPairRDDFunctionsImpl[K, V] =
    proxyOps[SPairRDDFunctionsImpl[K, V]](p)

  implicit class ExtendedSPairRDDFunctionsImpl[K, V](p: Rep[SPairRDDFunctionsImpl[K, V]])(implicit eK: Elem[K], eV: Elem[V]) {
    def toData: Rep[SPairRDDFunctionsImplData[K, V]] = isoSPairRDDFunctionsImpl(eK, eV).from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoSPairRDDFunctionsImpl[K, V](implicit eK: Elem[K], eV: Elem[V]): Iso[SPairRDDFunctionsImplData[K, V], SPairRDDFunctionsImpl[K, V]] =
    new SPairRDDFunctionsImplIso[K, V]

  // 6) smart constructor and deconstructor
  def mkSPairRDDFunctionsImpl[K, V](wrappedValueOfBaseType: Rep[PairRDDFunctions[K,V]])(implicit eK: Elem[K], eV: Elem[V]): Rep[SPairRDDFunctionsImpl[K, V]]
  def unmkSPairRDDFunctionsImpl[K:Elem, V:Elem](p: Rep[SPairRDDFunctionsImpl[K, V]]): Option[(Rep[PairRDDFunctions[K,V]])]
}

// Seq -----------------------------------
trait PairRDDFunctionssSeq extends PairRDDFunctionssAbs with PairRDDFunctionssDsl with ScalanSeq
{ self: SparkDslSeq =>
  lazy val SPairRDDFunctions: Rep[SPairRDDFunctionsCompanionAbs] = new SPairRDDFunctionsCompanionAbs with UserTypeSeq[SPairRDDFunctionsCompanionAbs, SPairRDDFunctionsCompanionAbs] {
    lazy val selfType = element[SPairRDDFunctionsCompanionAbs]
    
    override def apply[K:Elem, V:Elem](rdd: Rep[RDD[(K,V)]]): Rep[PairRDDFunctions[K,V]] =
      new PairRDDFunctions[K, V](rdd)

  }

    // override proxy if we deal with BaseTypeEx
  override def proxyPairRDDFunctions[K:Elem, V:Elem](p: Rep[PairRDDFunctions[K, V]]): SPairRDDFunctions[K, V] =
    proxyOpsEx[PairRDDFunctions[K, V],SPairRDDFunctions[K, V], SeqSPairRDDFunctionsImpl[K, V]](p, bt => SeqSPairRDDFunctionsImpl(bt))

    implicit def PairRDDFunctionsElement[K:Elem:WeakTypeTag, V:Elem:WeakTypeTag]: Elem[PairRDDFunctions[K, V]] = new SeqBaseElemEx[PairRDDFunctions[K, V], SPairRDDFunctions[K, V]](element[SPairRDDFunctions[K, V]])

  case class SeqSPairRDDFunctionsImpl[K, V]
      (override val wrappedValueOfBaseType: Rep[PairRDDFunctions[K,V]])
      (implicit eK: Elem[K], eV: Elem[V])
    extends SPairRDDFunctionsImpl[K, V](wrappedValueOfBaseType)
       with SeqSPairRDDFunctions[K, V] with UserTypeSeq[SPairRDDFunctions[K,V], SPairRDDFunctionsImpl[K, V]] {
    lazy val selfType = element[SPairRDDFunctionsImpl[K, V]].asInstanceOf[Elem[SPairRDDFunctions[K,V]]]
    
    override def partitionBy(partitioner: Rep[Partitioner]): Rep[RDD[(K,V)]] =
      wrappedValueOfBaseType.partitionBy(partitioner)

    
    override def values: Rep[RDD[V]] =
      wrappedValueOfBaseType.values

  }
  lazy val SPairRDDFunctionsImpl = new SPairRDDFunctionsImplCompanionAbs with UserTypeSeq[SPairRDDFunctionsImplCompanionAbs, SPairRDDFunctionsImplCompanionAbs] {
    lazy val selfType = element[SPairRDDFunctionsImplCompanionAbs]
  }

  def mkSPairRDDFunctionsImpl[K, V]
      (wrappedValueOfBaseType: Rep[PairRDDFunctions[K,V]])(implicit eK: Elem[K], eV: Elem[V]) =
      new SeqSPairRDDFunctionsImpl[K, V](wrappedValueOfBaseType)
  def unmkSPairRDDFunctionsImpl[K:Elem, V:Elem](p: Rep[SPairRDDFunctionsImpl[K, V]]) =
    Some((p.wrappedValueOfBaseType))
}

// Exp -----------------------------------
trait PairRDDFunctionssExp extends PairRDDFunctionssAbs with PairRDDFunctionssDsl with ScalanExp
{ self: SparkDslExp =>
  lazy val SPairRDDFunctions: Rep[SPairRDDFunctionsCompanionAbs] = new SPairRDDFunctionsCompanionAbs with UserTypeDef[SPairRDDFunctionsCompanionAbs, SPairRDDFunctionsCompanionAbs] {
    lazy val selfType = element[SPairRDDFunctionsCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  implicit def PairRDDFunctionsElement[K:Elem:WeakTypeTag, V:Elem:WeakTypeTag]: Elem[PairRDDFunctions[K, V]] = new ExpBaseElemEx[PairRDDFunctions[K, V], SPairRDDFunctions[K, V]](element[SPairRDDFunctions[K, V]])

  case class ExpSPairRDDFunctionsImpl[K, V]
      (override val wrappedValueOfBaseType: Rep[PairRDDFunctions[K,V]])
      (implicit eK: Elem[K], eV: Elem[V])
    extends SPairRDDFunctionsImpl[K, V](wrappedValueOfBaseType) with UserTypeDef[SPairRDDFunctions[K,V], SPairRDDFunctionsImpl[K, V]] {
    lazy val selfType = element[SPairRDDFunctionsImpl[K, V]].asInstanceOf[Elem[SPairRDDFunctions[K,V]]]
    override def mirror(t: Transformer) = ExpSPairRDDFunctionsImpl[K, V](t(wrappedValueOfBaseType))
  }

  lazy val SPairRDDFunctionsImpl: Rep[SPairRDDFunctionsImplCompanionAbs] = new SPairRDDFunctionsImplCompanionAbs with UserTypeDef[SPairRDDFunctionsImplCompanionAbs, SPairRDDFunctionsImplCompanionAbs] {
    lazy val selfType = element[SPairRDDFunctionsImplCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object SPairRDDFunctionsImplMethods {

  }



  def mkSPairRDDFunctionsImpl[K, V]
    (wrappedValueOfBaseType: Rep[PairRDDFunctions[K,V]])(implicit eK: Elem[K], eV: Elem[V]) =
    new ExpSPairRDDFunctionsImpl[K, V](wrappedValueOfBaseType)
  def unmkSPairRDDFunctionsImpl[K:Elem, V:Elem](p: Rep[SPairRDDFunctionsImpl[K, V]]) =
    Some((p.wrappedValueOfBaseType))

  object SPairRDDFunctionsMethods {
    object partitionBy {
      def unapply(d: Def[_]): Option[(Rep[SPairRDDFunctions[K, V]], Rep[Partitioner]) forSome {type K; type V}] = d match {
        case MethodCall(receiver, method, Seq(partitioner, _*)) if receiver.elem.isInstanceOf[SPairRDDFunctionsElem[_, _, _, _]] && method.getName == "partitionBy" =>
          Some((receiver, partitioner)).asInstanceOf[Option[(Rep[SPairRDDFunctions[K, V]], Rep[Partitioner]) forSome {type K; type V}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SPairRDDFunctions[K, V]], Rep[Partitioner]) forSome {type K; type V}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object reduceByKey {
      def unapply(d: Def[_]): Option[(Rep[SPairRDDFunctions[K, V]], Rep[((V,V)) => V]) forSome {type K; type V}] = d match {
        case MethodCall(receiver, method, Seq(func, _*)) if receiver.elem.isInstanceOf[SPairRDDFunctionsElem[_, _, _, _]] && method.getName == "reduceByKey" =>
          Some((receiver, func)).asInstanceOf[Option[(Rep[SPairRDDFunctions[K, V]], Rep[((V,V)) => V]) forSome {type K; type V}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SPairRDDFunctions[K, V]], Rep[((V,V)) => V]) forSome {type K; type V}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object values {
      def unapply(d: Def[_]): Option[Rep[SPairRDDFunctions[K, V]] forSome {type K; type V}] = d match {
        case MethodCall(receiver, method, _) if receiver.elem.isInstanceOf[SPairRDDFunctionsElem[_, _, _, _]] && method.getName == "values" =>
          Some(receiver).asInstanceOf[Option[Rep[SPairRDDFunctions[K, V]] forSome {type K; type V}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SPairRDDFunctions[K, V]] forSome {type K; type V}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object SPairRDDFunctionsCompanionMethods {
    object apply {
      def unapply(d: Def[_]): Option[Rep[RDD[(K,V)]] forSome {type K; type V}] = d match {
        case MethodCall(receiver, method, Seq(rdd, _*)) if receiver.elem.isInstanceOf[SPairRDDFunctionsCompanionElem] && method.getName == "apply" =>
          Some(rdd).asInstanceOf[Option[Rep[RDD[(K,V)]] forSome {type K; type V}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[RDD[(K,V)]] forSome {type K; type V}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }
}
