package scalan.spark
package impl

import org.apache.spark.rdd._
import scalan._
import scalan.common.Default
import org.apache.spark.Partitioner
import scala.reflect.runtime.universe._
import scalan.common.Default

// Abs -----------------------------------
trait PairRDDFunctionssAbs extends ScalanCommunityDsl with PairRDDFunctionss {
  self: SparkDsl =>
  // single proxy for each type family
  implicit def proxySPairRDDFunctions[K, V](p: Rep[SPairRDDFunctions[K, V]]): SPairRDDFunctions[K, V] = {
    implicit val tag = weakTypeTag[SPairRDDFunctions[K, V]]
    proxyOps[SPairRDDFunctions[K, V]](p)(TagImplicits.typeTagToClassTag[SPairRDDFunctions[K, V]])
  }
  // BaseTypeEx proxy
  //implicit def proxyPairRDDFunctions[K:Elem, V:Elem](p: Rep[PairRDDFunctions[K, V]]): SPairRDDFunctions[K, V] =
  //  proxyOps[SPairRDDFunctions[K, V]](p.asRep[SPairRDDFunctions[K, V]])

  implicit def unwrapValueOfSPairRDDFunctions[K, V](w: Rep[SPairRDDFunctions[K, V]]): Rep[PairRDDFunctions[K, V]] = w.wrappedValueOfBaseType

  implicit def defaultSPairRDDFunctionsElem[K:Elem, V:Elem]: Elem[SPairRDDFunctions[K, V]] = element[SPairRDDFunctionsImpl[K, V]].asElem[SPairRDDFunctions[K, V]]
  implicit def PairRDDFunctionsElement[K:Elem:WeakTypeTag, V:Elem:WeakTypeTag]: Elem[PairRDDFunctions[K, V]]

  abstract class SPairRDDFunctionsElem[K, V, From, To <: SPairRDDFunctions[K, V]](iso: Iso[From, To])(implicit eK: Elem[K], eV: Elem[V])
    extends ViewElem[From, To](iso) {
    override def convert(x: Rep[Reifiable[_]]) = convertSPairRDDFunctions(x.asRep[SPairRDDFunctions[K, V]])
    def convertSPairRDDFunctions(x : Rep[SPairRDDFunctions[K, V]]): Rep[To]
  }
  trait SPairRDDFunctionsCompanionElem extends CompanionElem[SPairRDDFunctionsCompanionAbs]
  implicit lazy val SPairRDDFunctionsCompanionElem: SPairRDDFunctionsCompanionElem = new SPairRDDFunctionsCompanionElem {
    lazy val tag = weakTypeTag[SPairRDDFunctionsCompanionAbs]
    protected def getDefaultRep = SPairRDDFunctions
  }

  abstract class SPairRDDFunctionsCompanionAbs extends CompanionBase[SPairRDDFunctionsCompanionAbs] with SPairRDDFunctionsCompanion {
    override def toString = "SPairRDDFunctions"

    def apply[K:Elem, V:Elem](rdd: Rep[SRDD[(K,V)]]): Rep[SPairRDDFunctions[K,V]] =
      newObjEx(classOf[SPairRDDFunctions[K, V]], List(rdd.asRep[Any]))
  }
  def SPairRDDFunctions: Rep[SPairRDDFunctionsCompanionAbs]
  implicit def proxySPairRDDFunctionsCompanion(p: Rep[SPairRDDFunctionsCompanion]): SPairRDDFunctionsCompanion = {
    proxyOps[SPairRDDFunctionsCompanion](p)
  }

  // default wrapper implementation
  abstract class SPairRDDFunctionsImpl[K, V](val wrappedValueOfBaseType: Rep[PairRDDFunctions[K, V]])(implicit val eK: Elem[K], val eV: Elem[V]) extends SPairRDDFunctions[K, V] {
    def keys: Rep[SRDD[K]] =
      methodCallEx[SRDD[K]](self,
        this.getClass.getMethod("keys"),
        List())

    def values: Rep[SRDD[V]] =
      methodCallEx[SRDD[V]](self,
        this.getClass.getMethod("values"),
        List())

    def partitionBy(partitioner: Rep[SPartitioner]): Rep[SRDD[(K,V)]] =
      methodCallEx[SRDD[(K,V)]](self,
        this.getClass.getMethod("partitionBy", classOf[AnyRef]),
        List(partitioner.asInstanceOf[AnyRef]))

    def reduceByKey(func: Rep[((V,V)) => V]): Rep[SRDD[(K,V)]] =
      methodCallEx[SRDD[(K,V)]](self,
        this.getClass.getMethod("reduceByKey", classOf[AnyRef]),
        List(func.asInstanceOf[AnyRef]))
  }
  trait SPairRDDFunctionsImplCompanion
  // elem for concrete class
  class SPairRDDFunctionsImplElem[K, V](iso: Iso[SPairRDDFunctionsImplData[K, V], SPairRDDFunctionsImpl[K, V]])(implicit val eK: Elem[K], val eV: Elem[V])
    extends SPairRDDFunctionsElem[K, V, SPairRDDFunctionsImplData[K, V], SPairRDDFunctionsImpl[K, V]](iso) {
    def convertSPairRDDFunctions(x: Rep[SPairRDDFunctions[K, V]]) = SPairRDDFunctionsImpl(x.wrappedValueOfBaseType)
  }

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
    lazy val defaultRepTo = Default.defaultVal[Rep[SPairRDDFunctionsImpl[K, V]]](SPairRDDFunctionsImpl(DefaultOfPairRDDFunctions[K, V].value))
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
    lazy val tag = weakTypeTag[SPairRDDFunctionsImplCompanionAbs]
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
trait PairRDDFunctionssSeq extends PairRDDFunctionssDsl with ScalanCommunityDslSeq {
  self: SparkDslSeq =>
  lazy val SPairRDDFunctions: Rep[SPairRDDFunctionsCompanionAbs] = new SPairRDDFunctionsCompanionAbs with UserTypeSeq[SPairRDDFunctionsCompanionAbs, SPairRDDFunctionsCompanionAbs] {
    lazy val selfType = element[SPairRDDFunctionsCompanionAbs]

    override def apply[K:Elem, V:Elem](rdd: Rep[SRDD[(K,V)]]): Rep[SPairRDDFunctions[K,V]] =
      SPairRDDFunctionsImpl(new PairRDDFunctions[K, V](rdd))
  }

    // override proxy if we deal with BaseTypeEx
  //override def proxyPairRDDFunctions[K:Elem, V:Elem](p: Rep[PairRDDFunctions[K, V]]): SPairRDDFunctions[K, V] =
  //  proxyOpsEx[PairRDDFunctions[K, V],SPairRDDFunctions[K, V], SeqSPairRDDFunctionsImpl[K, V]](p, bt => SeqSPairRDDFunctionsImpl(bt))

    implicit def PairRDDFunctionsElement[K:Elem:WeakTypeTag, V:Elem:WeakTypeTag]: Elem[PairRDDFunctions[K, V]] = new SeqBaseElemEx[PairRDDFunctions[K, V], SPairRDDFunctions[K, V]](element[SPairRDDFunctions[K, V]])(weakTypeTag[PairRDDFunctions[K, V]], DefaultOfPairRDDFunctions[K, V])

  case class SeqSPairRDDFunctionsImpl[K, V]
      (override val wrappedValueOfBaseType: Rep[PairRDDFunctions[K,V]])
      (implicit eK: Elem[K], eV: Elem[V])
    extends SPairRDDFunctionsImpl[K, V](wrappedValueOfBaseType)
        with UserTypeSeq[SPairRDDFunctions[K,V], SPairRDDFunctionsImpl[K, V]] {
    lazy val selfType = element[SPairRDDFunctionsImpl[K, V]].asInstanceOf[Elem[SPairRDDFunctions[K,V]]]
    override def keys: Rep[SRDD[K]] =
      wrappedValueOfBaseType.keys

    override def values: Rep[SRDD[V]] =
      wrappedValueOfBaseType.values

    override def partitionBy(partitioner: Rep[SPartitioner]): Rep[SRDD[(K,V)]] =
      wrappedValueOfBaseType.partitionBy(partitioner)

    override def reduceByKey(func: Rep[((V,V)) => V]): Rep[SRDD[(K,V)]] =
      wrappedValueOfBaseType.reduceByKey(scala.Function.untupled(func))
  }
  lazy val SPairRDDFunctionsImpl = new SPairRDDFunctionsImplCompanionAbs with UserTypeSeq[SPairRDDFunctionsImplCompanionAbs, SPairRDDFunctionsImplCompanionAbs] {
    lazy val selfType = element[SPairRDDFunctionsImplCompanionAbs]
  }

  def mkSPairRDDFunctionsImpl[K, V]
      (wrappedValueOfBaseType: Rep[PairRDDFunctions[K,V]])(implicit eK: Elem[K], eV: Elem[V]) =
      new SeqSPairRDDFunctionsImpl[K, V](wrappedValueOfBaseType)
  def unmkSPairRDDFunctionsImpl[K:Elem, V:Elem](p: Rep[SPairRDDFunctionsImpl[K, V]]) =
    Some((p.wrappedValueOfBaseType))

  implicit def wrapPairRDDFunctionsToSPairRDDFunctions[K:Elem, V:Elem](v: PairRDDFunctions[K, V]): SPairRDDFunctions[K, V] = SPairRDDFunctionsImpl(v)
}

// Exp -----------------------------------
trait PairRDDFunctionssExp extends PairRDDFunctionssDsl with ScalanCommunityDslExp {
  self: SparkDslExp =>
  lazy val SPairRDDFunctions: Rep[SPairRDDFunctionsCompanionAbs] = new SPairRDDFunctionsCompanionAbs with UserTypeDef[SPairRDDFunctionsCompanionAbs, SPairRDDFunctionsCompanionAbs] {
    lazy val selfType = element[SPairRDDFunctionsCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  implicit def PairRDDFunctionsElement[K:Elem:WeakTypeTag, V:Elem:WeakTypeTag]: Elem[PairRDDFunctions[K, V]] = new ExpBaseElemEx[PairRDDFunctions[K, V], SPairRDDFunctions[K, V]](element[SPairRDDFunctions[K, V]])(weakTypeTag[PairRDDFunctions[K, V]], DefaultOfPairRDDFunctions[K, V])
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
    object wrappedValueOfBaseType {
      def unapply(d: Def[_]): Option[Rep[SPairRDDFunctions[K, V]] forSome {type K; type V}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SPairRDDFunctionsElem[_, _, _, _]] && method.getName == "wrappedValueOfBaseType" =>
          Some(receiver).asInstanceOf[Option[Rep[SPairRDDFunctions[K, V]] forSome {type K; type V}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SPairRDDFunctions[K, V]] forSome {type K; type V}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object keys {
      def unapply(d: Def[_]): Option[Rep[SPairRDDFunctions[K, V]] forSome {type K; type V}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SPairRDDFunctionsElem[_, _, _, _]] && method.getName == "keys" =>
          Some(receiver).asInstanceOf[Option[Rep[SPairRDDFunctions[K, V]] forSome {type K; type V}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SPairRDDFunctions[K, V]] forSome {type K; type V}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object values {
      def unapply(d: Def[_]): Option[Rep[SPairRDDFunctions[K, V]] forSome {type K; type V}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SPairRDDFunctionsElem[_, _, _, _]] && method.getName == "values" =>
          Some(receiver).asInstanceOf[Option[Rep[SPairRDDFunctions[K, V]] forSome {type K; type V}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SPairRDDFunctions[K, V]] forSome {type K; type V}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object partitionBy {
      def unapply(d: Def[_]): Option[(Rep[SPairRDDFunctions[K, V]], Rep[SPartitioner]) forSome {type K; type V}] = d match {
        case MethodCall(receiver, method, Seq(partitioner, _*), _) if receiver.elem.isInstanceOf[SPairRDDFunctionsElem[_, _, _, _]] && method.getName == "partitionBy" =>
          Some((receiver, partitioner)).asInstanceOf[Option[(Rep[SPairRDDFunctions[K, V]], Rep[SPartitioner]) forSome {type K; type V}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SPairRDDFunctions[K, V]], Rep[SPartitioner]) forSome {type K; type V}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object reduceByKey {
      def unapply(d: Def[_]): Option[(Rep[SPairRDDFunctions[K, V]], Rep[((V,V)) => V]) forSome {type K; type V}] = d match {
        case MethodCall(receiver, method, Seq(func, _*), _) if receiver.elem.isInstanceOf[SPairRDDFunctionsElem[_, _, _, _]] && method.getName == "reduceByKey" =>
          Some((receiver, func)).asInstanceOf[Option[(Rep[SPairRDDFunctions[K, V]], Rep[((V,V)) => V]) forSome {type K; type V}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SPairRDDFunctions[K, V]], Rep[((V,V)) => V]) forSome {type K; type V}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object SPairRDDFunctionsCompanionMethods {
    object apply {
      def unapply(d: Def[_]): Option[Rep[SRDD[(K,V)]] forSome {type K; type V}] = d match {
        case MethodCall(receiver, method, Seq(rdd, _*), _) if receiver.elem.isInstanceOf[SPairRDDFunctionsCompanionElem] && method.getName == "apply" =>
          Some(rdd).asInstanceOf[Option[Rep[SRDD[(K,V)]] forSome {type K; type V}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SRDD[(K,V)]] forSome {type K; type V}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }
}
