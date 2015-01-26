package scalan.spark
package impl

import scalan._
import org.apache.spark.rdd.PairRDDFunctions
import scala.reflect.runtime.universe._
import scalan.common.Default

trait PairRDDsAbs extends Scalan with PairRDDs
{ self: PairRDDsDsl =>
  // single proxy for each type family
  implicit def proxySPairRDD[K, V](p: Rep[SPairRDD[K, V]]): SPairRDD[K, V] =
    proxyOps[SPairRDD[K, V]](p)
  // BaseTypeEx proxy
  implicit def proxyPairRDDFunctions[K:Elem, V:Elem](p: Rep[PairRDDFunctions[K, V]]): SPairRDD[K, V] =
    proxyOps[SPairRDD[K, V]](p.asRep[SPairRDD[K, V]])

  implicit def defaultSPairRDDElem[K:Elem, V:Elem]: Elem[SPairRDD[K, V]] = element[SPairRDDImpl[K, V]].asElem[SPairRDD[K, V]]
  implicit def PairRDDFunctionsElement[K:Elem:WeakTypeTag, V:Elem:WeakTypeTag]: Elem[PairRDDFunctions[K, V]]

  abstract class SPairRDDElem[K, V, From, To <: SPairRDD[K, V]](iso: Iso[From, To]) extends ViewElem[From, To]()(iso)

  trait SPairRDDCompanionElem extends CompanionElem[SPairRDDCompanionAbs]
  implicit lazy val SPairRDDCompanionElem: SPairRDDCompanionElem = new SPairRDDCompanionElem {
    lazy val tag = typeTag[SPairRDDCompanionAbs]
    lazy val getDefaultRep = Default.defaultVal(SPairRDD)
    //def getDefaultRep = defaultRep
  }

  abstract class SPairRDDCompanionAbs extends CompanionBase[SPairRDDCompanionAbs] with SPairRDDCompanion {
    override def toString = "SPairRDD"
    
  }
  def SPairRDD: Rep[SPairRDDCompanionAbs]
  implicit def proxySPairRDDCompanion(p: Rep[SPairRDDCompanion]): SPairRDDCompanion = {
    proxyOps[SPairRDDCompanion](p)
  }

  //default wrapper implementation
    abstract class SPairRDDImpl[K, V](val value: Rep[PairRDDFunctions[K, V]])(implicit val eK: Elem[K], val eV: Elem[V]) extends SPairRDD[K, V] {
    
    def partitionBy(partitioner: SPartitioner): RepPairRDD[K,V] =
      methodCallEx[SPairRDD[K,V]](self,
        this.getClass.getMethod("partitionBy", classOf[AnyRef]),
        List(partitioner.asInstanceOf[AnyRef]))

    
    def reduceByKey(func: Rep[((V,V)) => V]): RepPairRDD[K,V] =
      methodCallEx[SPairRDD[K,V]](self,
        this.getClass.getMethod("reduceByKey", classOf[AnyRef]),
        List(func.asInstanceOf[AnyRef]))

  }
  trait SPairRDDImplCompanion
  // elem for concrete class
  class SPairRDDImplElem[K, V](iso: Iso[SPairRDDImplData[K, V], SPairRDDImpl[K, V]]) extends SPairRDDElem[K, V, SPairRDDImplData[K, V], SPairRDDImpl[K, V]](iso)

  // state representation type
  type SPairRDDImplData[K, V] = PairRDDFunctions[K,V]

  // 3) Iso for concrete class
  class SPairRDDImplIso[K, V](implicit eK: Elem[K], eV: Elem[V])
    extends Iso[SPairRDDImplData[K, V], SPairRDDImpl[K, V]] {
    override def from(p: Rep[SPairRDDImpl[K, V]]) =
      unmkSPairRDDImpl(p) match {
        case Some((value)) => value
        case None => !!!
      }
    override def to(p: Rep[PairRDDFunctions[K,V]]) = {
      val value = p
      SPairRDDImpl(value)
    }
    lazy val tag = {
      weakTypeTag[SPairRDDImpl[K, V]]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[SPairRDDImpl[K, V]]](SPairRDDImpl(Default.defaultOf[PairRDDFunctions[K,V]]))
    lazy val eTo = new SPairRDDImplElem[K, V](this)
  }
  // 4) constructor and deconstructor
  abstract class SPairRDDImplCompanionAbs extends CompanionBase[SPairRDDImplCompanionAbs] with SPairRDDImplCompanion {
    override def toString = "SPairRDDImpl"

    def apply[K, V](value: Rep[PairRDDFunctions[K,V]])(implicit eK: Elem[K], eV: Elem[V]): Rep[SPairRDDImpl[K, V]] =
      mkSPairRDDImpl(value)
    def unapply[K:Elem, V:Elem](p: Rep[SPairRDDImpl[K, V]]) = unmkSPairRDDImpl(p)
  }
  def SPairRDDImpl: Rep[SPairRDDImplCompanionAbs]
  implicit def proxySPairRDDImplCompanion(p: Rep[SPairRDDImplCompanionAbs]): SPairRDDImplCompanionAbs = {
    proxyOps[SPairRDDImplCompanionAbs](p)
  }

  class SPairRDDImplCompanionElem extends CompanionElem[SPairRDDImplCompanionAbs] {
    lazy val tag = typeTag[SPairRDDImplCompanionAbs]
    lazy val getDefaultRep = Default.defaultVal(SPairRDDImpl)
    //def getDefaultRep = defaultRep
  }
  implicit lazy val SPairRDDImplCompanionElem: SPairRDDImplCompanionElem = new SPairRDDImplCompanionElem

  implicit def proxySPairRDDImpl[K, V](p: Rep[SPairRDDImpl[K, V]]): SPairRDDImpl[K, V] =
    proxyOps[SPairRDDImpl[K, V]](p)

  implicit class ExtendedSPairRDDImpl[K, V](p: Rep[SPairRDDImpl[K, V]])(implicit eK: Elem[K], eV: Elem[V]) {
    def toData: Rep[SPairRDDImplData[K, V]] = isoSPairRDDImpl(eK, eV).from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoSPairRDDImpl[K, V](implicit eK: Elem[K], eV: Elem[V]): Iso[SPairRDDImplData[K, V], SPairRDDImpl[K, V]] =
    new SPairRDDImplIso[K, V]

  // 6) smart constructor and deconstructor
  def mkSPairRDDImpl[K, V](value: Rep[PairRDDFunctions[K,V]])(implicit eK: Elem[K], eV: Elem[V]): Rep[SPairRDDImpl[K, V]]
  def unmkSPairRDDImpl[K:Elem, V:Elem](p: Rep[SPairRDDImpl[K, V]]): Option[(Rep[PairRDDFunctions[K,V]])]
}

trait PairRDDsSeq extends PairRDDsAbs with PairRDDsDsl with ScalanSeq { self: PairRDDsDslSeq =>
  lazy val SPairRDD: Rep[SPairRDDCompanionAbs] = new SPairRDDCompanionAbs with UserTypeSeq[SPairRDDCompanionAbs, SPairRDDCompanionAbs] {
    lazy val selfType = element[SPairRDDCompanionAbs]
    
  }

    // override proxy if we deal with BaseTypeEx
  override def proxyPairRDDFunctions[K:Elem, V:Elem](p: Rep[PairRDDFunctions[K, V]]): SPairRDD[K, V] =
    proxyOpsEx[PairRDDFunctions[K, V],SPairRDD[K, V], SeqSPairRDDImpl[K, V]](p, bt => SeqSPairRDDImpl(bt))

    implicit def PairRDDFunctionsElement[K:Elem:WeakTypeTag, V:Elem:WeakTypeTag]: Elem[PairRDDFunctions[K, V]] = new SeqBaseElemEx[PairRDDFunctions[K, V], SPairRDD[K, V]](element[SPairRDD[K, V]])

  case class SeqSPairRDDImpl[K, V]
      (override val value: Rep[PairRDDFunctions[K,V]])
      (implicit eK: Elem[K], eV: Elem[V])
    extends SPairRDDImpl[K, V](value)
        with UserTypeSeq[SPairRDD[K,V], SPairRDDImpl[K, V]] {
    lazy val selfType = element[SPairRDDImpl[K, V]].asInstanceOf[Elem[SPairRDD[K,V]]]
    
    override def partitionBy(partitioner: SPartitioner): RepPairRDD[K,V] =
      value.partitionBy(partitioner)

    
    override def reduceByKey(func: Rep[((V,V)) => V]): RepPairRDD[K,V] =
      value.reduceByKey(func)

  }
  lazy val SPairRDDImpl = new SPairRDDImplCompanionAbs with UserTypeSeq[SPairRDDImplCompanionAbs, SPairRDDImplCompanionAbs] {
    lazy val selfType = element[SPairRDDImplCompanionAbs]
  }

  def mkSPairRDDImpl[K, V]
      (value: Rep[PairRDDFunctions[K,V]])(implicit eK: Elem[K], eV: Elem[V]) =
      new SeqSPairRDDImpl[K, V](value)
  def unmkSPairRDDImpl[K:Elem, V:Elem](p: Rep[SPairRDDImpl[K, V]]) =
    Some((p.value))
}

trait PairRDDsExp extends PairRDDsAbs with PairRDDsDsl with ScalanExp {
  lazy val SPairRDD: Rep[SPairRDDCompanionAbs] = new SPairRDDCompanionAbs with UserTypeDef[SPairRDDCompanionAbs, SPairRDDCompanionAbs] {
    lazy val selfType = element[SPairRDDCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  implicit def PairRDDFunctionsElement[K:Elem:WeakTypeTag, V:Elem:WeakTypeTag]: Elem[PairRDDFunctions[K, V]] = new ExpBaseElemEx[PairRDDFunctions[K, V], SPairRDD[K, V]](element[SPairRDD[K, V]])

  case class ExpSPairRDDImpl[K, V]
      (override val value: Rep[PairRDDFunctions[K,V]])
      (implicit eK: Elem[K], eV: Elem[V])
    extends SPairRDDImpl[K, V](value) with UserTypeDef[SPairRDD[K,V], SPairRDDImpl[K, V]] {
    lazy val selfType = element[SPairRDDImpl[K, V]].asInstanceOf[Elem[SPairRDD[K,V]]]
    override def mirror(t: Transformer) = ExpSPairRDDImpl[K, V](t(value))
  }

  lazy val SPairRDDImpl: Rep[SPairRDDImplCompanionAbs] = new SPairRDDImplCompanionAbs with UserTypeDef[SPairRDDImplCompanionAbs, SPairRDDImplCompanionAbs] {
    lazy val selfType = element[SPairRDDImplCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object SPairRDDImplMethods {

  }



  def mkSPairRDDImpl[K, V]
    (value: Rep[PairRDDFunctions[K,V]])(implicit eK: Elem[K], eV: Elem[V]) =
    new ExpSPairRDDImpl[K, V](value)
  def unmkSPairRDDImpl[K:Elem, V:Elem](p: Rep[SPairRDDImpl[K, V]]) =
    Some((p.value))

  object SPairRDDMethods {
    object partitionBy {
      def unapply(d: Def[_]): Option[(Rep[SPairRDD[K, V]], SPartitioner) forSome {type K; type V}] = d match {
        case MethodCall(receiver, method, Seq(partitioner, _*)) if receiver.elem.isInstanceOf[SPairRDDElem[K, V, _, _] forSome {type K; type V}] && method.getName == "partitionBy" =>
          Some((receiver, partitioner)).asInstanceOf[Option[(Rep[SPairRDD[K, V]], SPartitioner) forSome {type K; type V}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SPairRDD[K, V]], SPartitioner) forSome {type K; type V}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object reduceByKey {
      def unapply(d: Def[_]): Option[(Rep[SPairRDD[K, V]], Rep[((V,V)) => V]) forSome {type K; type V}] = d match {
        case MethodCall(receiver, method, Seq(func, _*)) if receiver.elem.isInstanceOf[SPairRDDElem[K, V, _, _] forSome {type K; type V}] && method.getName == "reduceByKey" =>
          Some((receiver, func)).asInstanceOf[Option[(Rep[SPairRDD[K, V]], Rep[((V,V)) => V]) forSome {type K; type V}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SPairRDD[K, V]], Rep[((V,V)) => V]) forSome {type K; type V}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }


}
