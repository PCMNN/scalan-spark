package scalan.spark.parrays
package impl

import org.apache.spark.rdd.RDD
import scalan._
import scalan.common.OverloadHack.Overloaded1
import scalan.parrays._
import scalan.spark._
import scalan.common.Default
import scala.reflect.runtime.universe._
import scalan.common.Default

// Abs -----------------------------------
trait SparkArraysAbs extends Scalan with SparkArrays
{ self: SparkDsl =>
  // single proxy for each type family
  implicit def proxySparkArray[A](p: Rep[SparkArray[A]]): SparkArray[A] =
    proxyOps[SparkArray[A]](p)



  abstract class SparkArrayElem[A, From, To <: SparkArray[A]](iso: Iso[From, To]) extends ViewElem[From, To]()(iso)

  trait SparkArrayCompanionElem extends CompanionElem[SparkArrayCompanionAbs]
  implicit lazy val SparkArrayCompanionElem: SparkArrayCompanionElem = new SparkArrayCompanionElem {
    lazy val tag = typeTag[SparkArrayCompanionAbs]
    protected def getDefaultRep = SparkArray
  }

  abstract class SparkArrayCompanionAbs extends CompanionBase[SparkArrayCompanionAbs] with SparkArrayCompanion {
    override def toString = "SparkArray"
    
  }
  def SparkArray: Rep[SparkArrayCompanionAbs]
  implicit def proxySparkArrayCompanion(p: Rep[SparkArrayCompanion]): SparkArrayCompanion = {
    proxyOps[SparkArrayCompanion](p)
  }

  //default wrapper implementation
  
  // elem for concrete class
  class RDDArrayElem[A](iso: Iso[RDDArrayData[A], RDDArray[A]]) extends SparkArrayElem[A, RDDArrayData[A], RDDArray[A]](iso)

  // state representation type
  type RDDArrayData[A] = RDD[A]

  // 3) Iso for concrete class
  class RDDArrayIso[A](implicit eA: Elem[A])
    extends Iso[RDDArrayData[A], RDDArray[A]] {
    override def from(p: Rep[RDDArray[A]]) =
      unmkRDDArray(p) match {
        case Some((rdd)) => rdd
        case None => !!!
      }
    override def to(p: Rep[RDD[A]]) = {
      val rdd = p
      RDDArray(rdd)
    }
    lazy val tag = {
      weakTypeTag[RDDArray[A]]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[RDDArray[A]]](RDDArray(element[RDD[A]].defaultRepValue))
    lazy val eTo = new RDDArrayElem[A](this)
  }
  // 4) constructor and deconstructor
  abstract class RDDArrayCompanionAbs extends CompanionBase[RDDArrayCompanionAbs] with RDDArrayCompanion {
    override def toString = "RDDArray"

    def apply[A](rdd: Rep[RDD[A]])(implicit eA: Elem[A]): Rep[RDDArray[A]] =
      mkRDDArray(rdd)
    def unapply[A:Elem](p: Rep[RDDArray[A]]) = unmkRDDArray(p)
  }
  def RDDArray: Rep[RDDArrayCompanionAbs]
  implicit def proxyRDDArrayCompanion(p: Rep[RDDArrayCompanionAbs]): RDDArrayCompanionAbs = {
    proxyOps[RDDArrayCompanionAbs](p)
  }

  class RDDArrayCompanionElem extends CompanionElem[RDDArrayCompanionAbs] {
    lazy val tag = typeTag[RDDArrayCompanionAbs]
    protected def getDefaultRep = RDDArray
  }
  implicit lazy val RDDArrayCompanionElem: RDDArrayCompanionElem = new RDDArrayCompanionElem

  implicit def proxyRDDArray[A](p: Rep[RDDArray[A]]): RDDArray[A] =
    proxyOps[RDDArray[A]](p)

  implicit class ExtendedRDDArray[A](p: Rep[RDDArray[A]])(implicit eA: Elem[A]) {
    def toData: Rep[RDDArrayData[A]] = isoRDDArray(eA).from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoRDDArray[A](implicit eA: Elem[A]): Iso[RDDArrayData[A], RDDArray[A]] =
    new RDDArrayIso[A]

  // 6) smart constructor and deconstructor
  def mkRDDArray[A](rdd: Rep[RDD[A]])(implicit eA: Elem[A]): Rep[RDDArray[A]]
  def unmkRDDArray[A:Elem](p: Rep[RDDArray[A]]): Option[(Rep[RDD[A]])]
}

// Seq -----------------------------------
trait SparkArraysSeq extends SparkArraysAbs with SparkArraysDsl with ScalanSeq
{ self: SparkDslSeq =>
  lazy val SparkArray: Rep[SparkArrayCompanionAbs] = new SparkArrayCompanionAbs with UserTypeSeq[SparkArrayCompanionAbs, SparkArrayCompanionAbs] {
    lazy val selfType = element[SparkArrayCompanionAbs]
    
  }

  

  

  case class SeqRDDArray[A]
      (override val rdd: Rep[RDD[A]])
      (implicit eA: Elem[A])
    extends RDDArray[A](rdd)
        with UserTypeSeq[SparkArray[A], RDDArray[A]] {
    lazy val selfType = element[RDDArray[A]].asInstanceOf[Elem[SparkArray[A]]]
    
  }
  lazy val RDDArray = new RDDArrayCompanionAbs with UserTypeSeq[RDDArrayCompanionAbs, RDDArrayCompanionAbs] {
    lazy val selfType = element[RDDArrayCompanionAbs]
  }

  def mkRDDArray[A]
      (rdd: Rep[RDD[A]])(implicit eA: Elem[A]) =
      new SeqRDDArray[A](rdd)
  def unmkRDDArray[A:Elem](p: Rep[RDDArray[A]]) =
    Some((p.rdd))
}

// Exp -----------------------------------
trait SparkArraysExp extends SparkArraysAbs with SparkArraysDsl with ScalanExp
{ self: SparkDslExp =>
  lazy val SparkArray: Rep[SparkArrayCompanionAbs] = new SparkArrayCompanionAbs with UserTypeDef[SparkArrayCompanionAbs, SparkArrayCompanionAbs] {
    lazy val selfType = element[SparkArrayCompanionAbs]
    override def mirror(t: Transformer) = this
  }



  case class ExpRDDArray[A]
      (override val rdd: Rep[RDD[A]])
      (implicit eA: Elem[A])
    extends RDDArray[A](rdd) with UserTypeDef[SparkArray[A], RDDArray[A]] {
    lazy val selfType = element[RDDArray[A]].asInstanceOf[Elem[SparkArray[A]]]
    override def mirror(t: Transformer) = ExpRDDArray[A](t(rdd))
  }

  lazy val RDDArray: Rep[RDDArrayCompanionAbs] = new RDDArrayCompanionAbs with UserTypeDef[RDDArrayCompanionAbs, RDDArrayCompanionAbs] {
    lazy val selfType = element[RDDArrayCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object RDDArrayMethods {
    object elem {
      def unapply(d: Def[_]): Option[Rep[RDDArray[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _) if receiver.elem.isInstanceOf[RDDArrayElem[_]] && method.getName == "elem" =>
          Some(receiver).asInstanceOf[Option[Rep[RDDArray[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[RDDArray[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object length {
      def unapply(d: Def[_]): Option[Rep[RDDArray[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _) if receiver.elem.isInstanceOf[RDDArrayElem[_]] && method.getName == "length" =>
          Some(receiver).asInstanceOf[Option[Rep[RDDArray[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[RDDArray[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object arr {
      def unapply(d: Def[_]): Option[Rep[RDDArray[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _) if receiver.elem.isInstanceOf[RDDArrayElem[_]] && method.getName == "arr" =>
          Some(receiver).asInstanceOf[Option[Rep[RDDArray[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[RDDArray[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object apply {
      def unapply(d: Def[_]): Option[(Rep[RDDArray[A]], Rep[Int]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(i, _*)) if receiver.elem.isInstanceOf[RDDArrayElem[_]] && method.getName == "apply"&& method.getAnnotation(classOf[scalan.OverloadId]) == null =>
          Some((receiver, i)).asInstanceOf[Option[(Rep[RDDArray[A]], Rep[Int]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDArray[A]], Rep[Int]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object apply_many {
      def unapply(d: Def[_]): Option[(Rep[RDDArray[A]], Arr[Int]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(indices, _*)) if receiver.elem.isInstanceOf[RDDArrayElem[_]] && method.getName == "apply" && { val ann = method.getAnnotation(classOf[scalan.OverloadId]); ann != null && ann.value == "many" } =>
          Some((receiver, indices)).asInstanceOf[Option[(Rep[RDDArray[A]], Arr[Int]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDArray[A]], Arr[Int]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object mapBy {
      def unapply(d: Def[_]): Option[(Rep[RDDArray[A]], Rep[A => B]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(f, _*)) if receiver.elem.isInstanceOf[RDDArrayElem[_]] && method.getName == "mapBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[RDDArray[A]], Rep[A => B]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDArray[A]], Rep[A => B]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    // WARNING: Cannot generate matcher for method `map`: Method has function arguments f

    object slice {
      def unapply(d: Def[_]): Option[(Rep[RDDArray[A]], Rep[Int], Rep[Int]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(offset, length, _*)) if receiver.elem.isInstanceOf[RDDArrayElem[_]] && method.getName == "slice" =>
          Some((receiver, offset, length)).asInstanceOf[Option[(Rep[RDDArray[A]], Rep[Int], Rep[Int]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDArray[A]], Rep[Int], Rep[Int]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object reduce {
      def unapply(d: Def[_]): Option[(Rep[RDDArray[A]], RepMonoid[A]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(m, _*)) if receiver.elem.isInstanceOf[RDDArrayElem[_]] && method.getName == "reduce" =>
          Some((receiver, m)).asInstanceOf[Option[(Rep[RDDArray[A]], RepMonoid[A]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDArray[A]], RepMonoid[A]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object RDDArrayCompanionMethods {
    object apply {
      def unapply(d: Def[_]): Option[Rep[Array[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(arr, _*)) if receiver.elem.isInstanceOf[RDDArrayCompanionElem] && method.getName == "apply" =>
          Some(arr).asInstanceOf[Option[Rep[Array[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[Array[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object fromArray {
      def unapply(d: Def[_]): Option[(Rep[Array[A]], RepSparkContext) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(arr, sc, _*)) if receiver.elem.isInstanceOf[RDDArrayCompanionElem] && method.getName == "fromArray" =>
          Some((arr, sc)).asInstanceOf[Option[(Rep[Array[A]], RepSparkContext) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[Array[A]], RepSparkContext) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object defaultOf {
      def unapply(d: Def[_]): Option[Elem[A] forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(ea, _*)) if receiver.elem.isInstanceOf[RDDArrayCompanionElem] && method.getName == "defaultOf" =>
          Some(ea).asInstanceOf[Option[Elem[A] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Elem[A] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object replicate {
      def unapply(d: Def[_]): Option[(Rep[Int], Rep[A]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(len, v, _*)) if receiver.elem.isInstanceOf[RDDArrayCompanionElem] && method.getName == "replicate" =>
          Some((len, v)).asInstanceOf[Option[(Rep[Int], Rep[A]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[Int], Rep[A]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object singleton {
      def unapply(d: Def[_]): Option[Rep[A] forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(v, _*)) if receiver.elem.isInstanceOf[RDDArrayCompanionElem] && method.getName == "singleton" =>
          Some(v).asInstanceOf[Option[Rep[A] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[A] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  def mkRDDArray[A]
    (rdd: Rep[RDD[A]])(implicit eA: Elem[A]) =
    new ExpRDDArray[A](rdd)
  def unmkRDDArray[A:Elem](p: Rep[RDDArray[A]]) =
    Some((p.rdd))

  object SparkArrayMethods {

  }

  object SparkArrayCompanionMethods {

  }
}
