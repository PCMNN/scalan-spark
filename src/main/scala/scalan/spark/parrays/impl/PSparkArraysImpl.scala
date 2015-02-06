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
trait PSparkArraysAbs extends Scalan with PSparkArrays
{ self: SparkDsl with PArraysDsl =>
  // single proxy for each type family
  implicit def proxyRDDArrayCompanion(p: Rep[RDDArrayCompanion]): RDDArrayCompanion =
    proxyOps[RDDArrayCompanion](p)



  abstract class RDDArrayCompanionElem[From, To <: RDDArrayCompanion](iso: Iso[From, To]) extends ViewElem[From, To]()(iso)

  trait RDDArrayCompanionCompanionElem extends CompanionElem[RDDArrayCompanionCompanionAbs]
  implicit lazy val RDDArrayCompanionCompanionElem: RDDArrayCompanionCompanionElem = new RDDArrayCompanionCompanionElem {
    lazy val tag = typeTag[RDDArrayCompanionCompanionAbs]
    protected def getDefaultRep = RDDArrayCompanion
  }

  abstract class RDDArrayCompanionCompanionAbs extends CompanionBase[RDDArrayCompanionCompanionAbs] with RDDArrayCompanionCompanion {
    override def toString = "RDDArrayCompanion"
    
  }
  def RDDArrayCompanion: Rep[RDDArrayCompanionCompanionAbs]
  implicit def proxyRDDArrayCompanionCompanion(p: Rep[RDDArrayCompanionCompanion]): RDDArrayCompanionCompanion = {
    proxyOps[RDDArrayCompanionCompanion](p)
  }

  //default wrapper implementation
  
  // elem for concrete class
  class RDDArrayElem[A](iso: Iso[RDDArrayData[A], RDDArray[A]]) extends RDDArrayCompanionElem[A, RDDArrayData[A], RDDArray[A]](iso)

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
trait PSparkArraysSeq extends PSparkArraysAbs with PSparkArraysDsl with ScalanSeq
{ self: SparkDsl with PArraysDslSeq =>
  lazy val RDDArrayCompanion: Rep[RDDArrayCompanionCompanionAbs] = new RDDArrayCompanionCompanionAbs with UserTypeSeq[RDDArrayCompanionCompanionAbs, RDDArrayCompanionCompanionAbs] {
    lazy val selfType = element[RDDArrayCompanionCompanionAbs]
    
  }

  

  

  case class SeqRDDArray[A]
      (override val rdd: Rep[RDD[A]])
      (implicit eA: Elem[A])
    extends RDDArray[A](rdd)
        with UserTypeSeq[PArray[A], RDDArray[A]] {
    lazy val selfType = element[RDDArray[A]].asInstanceOf[Elem[PArray[A]]]
    
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
trait PSparkArraysExp extends PSparkArraysAbs with PSparkArraysDsl with ScalanExp
{ self: SparkDsl with PArraysDslExp =>
  lazy val RDDArrayCompanion: Rep[RDDArrayCompanionCompanionAbs] = new RDDArrayCompanionCompanionAbs with UserTypeDef[RDDArrayCompanionCompanionAbs, RDDArrayCompanionCompanionAbs] {
    lazy val selfType = element[RDDArrayCompanionCompanionAbs]
    override def mirror(t: Transformer) = this
  }



  case class ExpRDDArray[A]
      (override val rdd: Rep[RDD[A]])
      (implicit eA: Elem[A])
    extends RDDArray[A](rdd) with UserTypeDef[PArray[A], RDDArray[A]] {
    lazy val selfType = element[RDDArray[A]].asInstanceOf[Elem[PArray[A]]]
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

    // WARNING: Cannot generate matcher for method `apply`: Method's return type PA[A] is not a Rep

    // WARNING: Cannot generate matcher for method `mapBy`: Method's return type PA[B] is not a Rep

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
    // WARNING: Cannot generate matcher for method `apply`: Method's return type PA[A] is not a Rep

    // WARNING: Cannot generate matcher for method `fromArray`: Method's return type PA[A] is not a Rep

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

    // WARNING: Cannot generate matcher for method `replicate`: Method's return type PA[A] is not a Rep

    // WARNING: Cannot generate matcher for method `singleton`: Method's return type PA[A] is not a Rep
  }

  def mkRDDArray[A]
    (rdd: Rep[RDD[A]])(implicit eA: Elem[A]) =
    new ExpRDDArray[A](rdd)
  def unmkRDDArray[A:Elem](p: Rep[RDDArray[A]]) =
    Some((p.rdd))

  object RDDArrayCompanionMethods {
    // WARNING: Cannot generate matcher for method `apply`: Method's return type PA[A] is not a Rep

    // WARNING: Cannot generate matcher for method `fromArray`: Method's return type PA[A] is not a Rep

    object defaultOf {
      def unapply(d: Def[_]): Option[(Rep[RDDArrayCompanion], Elem[A]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(ea, _*)) if receiver.elem.isInstanceOf[RDDArrayCompanionElem[_, _]] && method.getName == "defaultOf" =>
          Some((receiver, ea)).asInstanceOf[Option[(Rep[RDDArrayCompanion], Elem[A]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDArrayCompanion], Elem[A]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    // WARNING: Cannot generate matcher for method `replicate`: Method's return type PA[A] is not a Rep

    // WARNING: Cannot generate matcher for method `singleton`: Method's return type PA[A] is not a Rep
  }


}
