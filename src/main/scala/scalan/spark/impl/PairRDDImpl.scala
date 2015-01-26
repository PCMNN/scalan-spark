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
  implicit def proxyPairRDDFunctions[K, V](p: Rep[PairRDDFunctions[K, V]]): SPairRDD[K, V] =
    proxyOps[SPairRDD[K, V]](p.asRep[SPairRDD[K, V]])
  implicit def PairRDDFunctionsElement[K:Elem, V:Elem]: Elem[PairRDDFunctions[K, V]] = new BaseElemEx[PairRDDFunctions[K, V], SPairRDD[K, V]](element[SPairRDD[K, V]])
  implicit lazy val DefaultOfPairRDDFunctions[K:Elem, V:Elem]: Default[PairRDDFunctions[K, V]] = SPairRDD.defaultVal

  abstract class SPairRDDElem[K, V, From, To <: SPairRDD[K, V]](iso: Iso[From, To]) extends ViewElem[From, To]()(iso)

  trait SPairRDDCompanionElem extends CompanionElem[SPairRDDCompanionAbs]
  implicit lazy val SPairRDDCompanionElem: SPairRDDCompanionElem = new SPairRDDCompanionElem {
    lazy val tag = typeTag[SPairRDDCompanionAbs]
    lazy val defaultRep = Default.defaultVal(SPairRDD)
  }

  abstract class SPairRDDCompanionAbs extends CompanionBase[SPairRDDCompanionAbs] with SPairRDDCompanion {
    override def toString = "SPairRDD"
  }
  def SPairRDD: Rep[SPairRDDCompanionAbs]
  implicit def proxySPairRDDCompanion(p: Rep[SPairRDDCompanion]): SPairRDDCompanion = {
    proxyOps[SPairRDDCompanion](p)
  }


}

trait PairRDDsSeq extends PairRDDsAbs with PairRDDsDsl with ScalanSeq {
  lazy val SPairRDD: Rep[SPairRDDCompanionAbs] = new SPairRDDCompanionAbs with UserTypeSeq[SPairRDDCompanionAbs, SPairRDDCompanionAbs] {
    lazy val selfType = element[SPairRDDCompanionAbs]
  }


}

trait PairRDDsExp extends PairRDDsAbs with PairRDDsDsl with ScalanExp {
  lazy val SPairRDD: Rep[SPairRDDCompanionAbs] = new SPairRDDCompanionAbs with UserTypeDef[SPairRDDCompanionAbs, SPairRDDCompanionAbs] {
    lazy val selfType = element[SPairRDDCompanionAbs]
    override def mirror(t: Transformer) = this
  }



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
