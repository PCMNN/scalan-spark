package scalan.spark.collections
package impl

import scala.annotation.unchecked.uncheckedVariance
import scalan.OverloadId
import scalan.common.Default
import scalan.common.OverloadHack.Overloaded1
import scalan.spark.{SparkDslExp, SparkDslSeq, SparkDsl}
import scala.reflect._
import scala.reflect.runtime.universe._
import scalan.common.Default

// Abs -----------------------------------
trait RDDCollectionsAbs extends RDDCollections with SparkDsl {
  self: SparkDsl with RDDCollectionsDsl =>
  // single proxy for each type family
  implicit def proxyIRDDCollection[A](p: Rep[IRDDCollection[A]]): IRDDCollection[A] = {
    proxyOps[IRDDCollection[A]](p)(classTag[IRDDCollection[A]])
  }

  // familyElem
  class IRDDCollectionElem[A, To <: IRDDCollection[A]](implicit val eA: Elem[A])
    extends CollectionElem[A, To] {
    override def isEntityType = true
    override def tag = {
      implicit val tagA = eA.tag
      weakTypeTag[IRDDCollection[A]].asInstanceOf[WeakTypeTag[To]]
    }
    override def convert(x: Rep[Reifiable[_]]) = convertIRDDCollection(x.asRep[IRDDCollection[A]])
    def convertIRDDCollection(x : Rep[IRDDCollection[A]]): Rep[To] = {
      //assert(x.selfType1.isInstanceOf[IRDDCollectionElem[_,_]])
      x.asRep[To]
    }
    override def getDefaultRep: Rep[To] = ???
  }

  implicit def iRDDCollectionElement[A](implicit eA: Elem[A]) =
    new IRDDCollectionElem[A, IRDDCollection[A]]()(eA)

  trait IRDDCollectionCompanionElem extends CompanionElem[IRDDCollectionCompanionAbs]
  implicit lazy val IRDDCollectionCompanionElem: IRDDCollectionCompanionElem = new IRDDCollectionCompanionElem {
    lazy val tag = weakTypeTag[IRDDCollectionCompanionAbs]
    protected def getDefaultRep = IRDDCollection
  }

  abstract class IRDDCollectionCompanionAbs extends CompanionBase[IRDDCollectionCompanionAbs] with IRDDCollectionCompanion {
    override def toString = "IRDDCollection"
  }
  def IRDDCollection: Rep[IRDDCollectionCompanionAbs]
  implicit def proxyIRDDCollectionCompanion(p: Rep[IRDDCollectionCompanion]): IRDDCollectionCompanion = {
    proxyOps[IRDDCollectionCompanion](p)
  }

  // single proxy for each type family
  implicit def proxyIRDDPairCollection[A, B](p: Rep[IRDDPairCollection[A, B]]): IRDDPairCollection[A, B] = {
    proxyOps[IRDDPairCollection[A, B]](p)(classTag[IRDDPairCollection[A, B]])
  }
  // familyElem
  class IRDDPairCollectionElem[A, B, To <: IRDDPairCollection[A, B]](implicit override val eA: Elem[A], override val eB: Elem[B])
    extends IPairCollectionElem[A, B, To] {
    override def isEntityType = true
    override def tag = {
      implicit val tagA = eA.tag
      implicit val tagB = eB.tag
      weakTypeTag[IRDDPairCollection[A, B]].asInstanceOf[WeakTypeTag[To]]
    }
    override def convert(x: Rep[Reifiable[_]]) = convertIRDDPairCollection(x.asRep[IRDDPairCollection[A, B]])
    def convertIRDDPairCollection(x : Rep[IRDDPairCollection[A, B]]): Rep[To] = {
      //assert(x.selfType1.isInstanceOf[IRDDPairCollectionElem[_,_,_]])
      x.asRep[To]
    }
    override def getDefaultRep: Rep[To] = ???
  }

  implicit def iRDDPairCollectionElement[A, B](implicit eA: Elem[A], eB: Elem[B]) =
    new IRDDPairCollectionElem[A, B, IRDDPairCollection[A, B]]()(eA, eB)

  // single proxy for each type family
  implicit def proxyIRDDNestedCollection[A](p: Rep[IRDDNestedCollection[A]]): IRDDNestedCollection[A] = {
    proxyOps[IRDDNestedCollection[A]](p)(classTag[IRDDNestedCollection[A]])
  }
  // familyElem
  class IRDDNestedCollectionElem[A, To <: IRDDNestedCollection[A]](implicit override val eA: Elem[A])
    extends INestedCollectionElem[A, To] {
    override def isEntityType = true
    override def tag = {
      implicit val tagA = eA.tag
      weakTypeTag[IRDDNestedCollection[A]].asInstanceOf[WeakTypeTag[To]]
    }
    override def convert(x: Rep[Reifiable[_]]) = convertIRDDNestedCollection(x.asRep[IRDDNestedCollection[A]])
    def convertIRDDNestedCollection(x : Rep[IRDDNestedCollection[A]]): Rep[To] = {
      //assert(x.selfType1.isInstanceOf[IRDDNestedCollectionElem[_,_]])
      x.asRep[To]
    }
    override def getDefaultRep: Rep[To] = ???
  }

  implicit def iRDDNestedCollectionElement[A](implicit eA: Elem[A]) =
    new IRDDNestedCollectionElem[A, IRDDNestedCollection[A]]()(eA)

  // elem for concrete class
  class RDDCollectionElem[A](val iso: Iso[RDDCollectionData[A], RDDCollection[A]])(implicit eA: Elem[A])
    extends IRDDCollectionElem[A, RDDCollection[A]]
    with ConcreteElem[RDDCollectionData[A], RDDCollection[A]] {
    override def convertIRDDCollection(x: Rep[IRDDCollection[A]]) = RDDCollection(x.rdd)
    override def getDefaultRep = super[ConcreteElem].getDefaultRep
    override lazy val tag = super[ConcreteElem].tag
  }

  // state representation type
  type RDDCollectionData[A] = SRDD[A]

  // 3) Iso for concrete class
  class RDDCollectionIso[A](implicit eA: Elem[A])
    extends Iso[RDDCollectionData[A], RDDCollection[A]] {
    override def from(p: Rep[RDDCollection[A]]) =
      unmkRDDCollection(p) match {
        case Some((rdd)) => rdd
        case None => !!!
      }
    override def to(p: Rep[SRDD[A]]) = {
      val rdd = p
      RDDCollection(rdd)
    }
    lazy val tag = {
      implicit val tagA = eA.tag
      weakTypeTag[RDDCollection[A]]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[RDDCollection[A]]](RDDCollection(element[SRDD[A]].defaultRepValue))
    lazy val eTo = new RDDCollectionElem[A](this)
  }
  // 4) constructor and deconstructor
  abstract class RDDCollectionCompanionAbs extends CompanionBase[RDDCollectionCompanionAbs] with RDDCollectionCompanion {
    override def toString = "RDDCollection"

    def apply[A](rdd: RepRDD[A])(implicit eA: Elem[A]): Rep[RDDCollection[A]] =
      mkRDDCollection(rdd)
    def unapply[A:Elem](p: Rep[RDDCollection[A]]) = unmkRDDCollection(p)
  }
  def RDDCollection: Rep[RDDCollectionCompanionAbs]
  implicit def proxyRDDCollectionCompanion(p: Rep[RDDCollectionCompanionAbs]): RDDCollectionCompanionAbs = {
    proxyOps[RDDCollectionCompanionAbs](p)
  }

  class RDDCollectionCompanionElem extends CompanionElem[RDDCollectionCompanionAbs] {
    lazy val tag = weakTypeTag[RDDCollectionCompanionAbs]
    protected def getDefaultRep = RDDCollection
  }
  implicit lazy val RDDCollectionCompanionElem: RDDCollectionCompanionElem = new RDDCollectionCompanionElem

  implicit def proxyRDDCollection[A](p: Rep[RDDCollection[A]]): RDDCollection[A] =
    proxyOps[RDDCollection[A]](p)

  implicit class ExtendedRDDCollection[A](p: Rep[RDDCollection[A]])(implicit eA: Elem[A]) {
    def toData: Rep[RDDCollectionData[A]] = isoRDDCollection(eA).from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoRDDCollection[A](implicit eA: Elem[A]): Iso[RDDCollectionData[A], RDDCollection[A]] =
    new RDDCollectionIso[A]

  // 6) smart constructor and deconstructor
  def mkRDDCollection[A](rdd: RepRDD[A])(implicit eA: Elem[A]): Rep[RDDCollection[A]]
  def unmkRDDCollection[A:Elem](p: Rep[RDDCollection[A]]): Option[(Rep[SRDD[A]])]

  // elem for concrete class
  class PairRDDCollectionElem[A, B](val iso: Iso[PairRDDCollectionData[A, B], PairRDDCollection[A, B]])(implicit eA: Elem[A], eB: Elem[B])
    extends IRDDPairCollectionElem[A, B, PairRDDCollection[A, B]]
    with ConcreteElem[PairRDDCollectionData[A, B], PairRDDCollection[A, B]] {
    override def convertIRDDPairCollection(x: Rep[IRDDPairCollection[A, B]]) = PairRDDCollection(x.pairRDD)
    override def getDefaultRep = super[ConcreteElem].getDefaultRep
    override lazy val tag = super[ConcreteElem].tag
  }

  // state representation type
  type PairRDDCollectionData[A, B] = SRDD[(A, B)]

  // 3) Iso for concrete class
  class PairRDDCollectionIso[A, B](implicit eA: Elem[A], eB: Elem[B])
    extends Iso[PairRDDCollectionData[A, B], PairRDDCollection[A, B]] {
    override def from(p: Rep[PairRDDCollection[A, B]]) =
      unmkPairRDDCollection(p) match {
        case Some((pairRDD)) => pairRDD
        case None => !!!
      }
    override def to(p: Rep[SRDD[(A, B)]]) = {
      val pairRDD = p
      PairRDDCollection(pairRDD)
    }
    lazy val tag = {
      implicit val tagA = eA.tag
      implicit val tagB = eB.tag
      weakTypeTag[PairRDDCollection[A, B]]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[PairRDDCollection[A, B]]](PairRDDCollection(element[SRDD[(A, B)]].defaultRepValue))
    lazy val eTo = new PairRDDCollectionElem[A, B](this)
  }
  // 4) constructor and deconstructor
  abstract class PairRDDCollectionCompanionAbs extends CompanionBase[PairRDDCollectionCompanionAbs] with PairRDDCollectionCompanion {
    override def toString = "PairRDDCollection"

    def apply[A, B](pairRDD: RepRDD[(A, B)])(implicit eA: Elem[A], eB: Elem[B]): Rep[PairRDDCollection[A, B]] =
      mkPairRDDCollection(pairRDD)
    def unapply[A:Elem, B:Elem](p: Rep[PairRDDCollection[A, B]]) = unmkPairRDDCollection(p)
  }
  def PairRDDCollection: Rep[PairRDDCollectionCompanionAbs]
  implicit def proxyPairRDDCollectionCompanion(p: Rep[PairRDDCollectionCompanionAbs]): PairRDDCollectionCompanionAbs = {
    proxyOps[PairRDDCollectionCompanionAbs](p)
  }

  class PairRDDCollectionCompanionElem extends CompanionElem[PairRDDCollectionCompanionAbs] {
    lazy val tag = weakTypeTag[PairRDDCollectionCompanionAbs]
    protected def getDefaultRep = PairRDDCollection
  }
  implicit lazy val PairRDDCollectionCompanionElem: PairRDDCollectionCompanionElem = new PairRDDCollectionCompanionElem

  implicit def proxyPairRDDCollection[A, B](p: Rep[PairRDDCollection[A, B]]): PairRDDCollection[A, B] =
    proxyOps[PairRDDCollection[A, B]](p)

  implicit class ExtendedPairRDDCollection[A, B](p: Rep[PairRDDCollection[A, B]])(implicit eA: Elem[A], eB: Elem[B]) {
    def toData: Rep[PairRDDCollectionData[A, B]] = isoPairRDDCollection(eA, eB).from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoPairRDDCollection[A, B](implicit eA: Elem[A], eB: Elem[B]): Iso[PairRDDCollectionData[A, B], PairRDDCollection[A, B]] =
    new PairRDDCollectionIso[A, B]

  // 6) smart constructor and deconstructor
  def mkPairRDDCollection[A, B](pairRDD: RepRDD[(A, B)])(implicit eA: Elem[A], eB: Elem[B]): Rep[PairRDDCollection[A, B]]
  def unmkPairRDDCollection[A:Elem, B:Elem](p: Rep[PairRDDCollection[A, B]]): Option[(Rep[SRDD[(A, B)]])]

  // elem for concrete class
  class RDDNestedCollectionElem[A](val iso: Iso[RDDNestedCollectionData[A], RDDNestedCollection[A]])(implicit eA: Elem[A])
    extends IRDDNestedCollectionElem[A, RDDNestedCollection[A]]
    with ConcreteElem[RDDNestedCollectionData[A], RDDNestedCollection[A]] {
    override def convertIRDDNestedCollection(x: Rep[IRDDNestedCollection[A]]) = RDDNestedCollection(x.values, x.segments)
    override def getDefaultRep = super[ConcreteElem].getDefaultRep
    override lazy val tag = super[ConcreteElem].tag
  }

  // state representation type
  type RDDNestedCollectionData[A] = (IRDDCollection[A], PairRDDCollection[Int,Int])

  // 3) Iso for concrete class
  class RDDNestedCollectionIso[A](implicit eA: Elem[A])
    extends Iso[RDDNestedCollectionData[A], RDDNestedCollection[A]] {
    override def from(p: Rep[RDDNestedCollection[A]]) =
      unmkRDDNestedCollection(p) match {
        case Some((values, segments)) => Pair(values, segments)
        case None => !!!
      }
    override def to(p: Rep[(IRDDCollection[A], PairRDDCollection[Int,Int])]) = {
      val Pair(values, segments) = p
      RDDNestedCollection(values, segments)
    }
    lazy val tag = {
      implicit val tagA = eA.tag
      weakTypeTag[RDDNestedCollection[A]]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[RDDNestedCollection[A]]](RDDNestedCollection(element[IRDDCollection[A]].defaultRepValue, element[PairRDDCollection[Int,Int]].defaultRepValue))
    lazy val eTo = new RDDNestedCollectionElem[A](this)
  }
  // 4) constructor and deconstructor
  abstract class RDDNestedCollectionCompanionAbs extends CompanionBase[RDDNestedCollectionCompanionAbs] with RDDNestedCollectionCompanion {
    override def toString = "RDDNestedCollection"
    def apply[A](p: Rep[RDDNestedCollectionData[A]])(implicit eA: Elem[A]): Rep[RDDNestedCollection[A]] =
      isoRDDNestedCollection(eA).to(p)
    def apply[A](values: Rep[IRDDCollection[A]], segments: Rep[PairRDDCollection[Int,Int]])(implicit eA: Elem[A]): Rep[RDDNestedCollection[A]] =
      mkRDDNestedCollection(values, segments)
    def unapply[A:Elem](p: Rep[RDDNestedCollection[A]]) = unmkRDDNestedCollection(p)
  }
  def RDDNestedCollection: Rep[RDDNestedCollectionCompanionAbs]
  implicit def proxyRDDNestedCollectionCompanion(p: Rep[RDDNestedCollectionCompanionAbs]): RDDNestedCollectionCompanionAbs = {
    proxyOps[RDDNestedCollectionCompanionAbs](p)
  }

  class RDDNestedCollectionCompanionElem extends CompanionElem[RDDNestedCollectionCompanionAbs] {
    lazy val tag = weakTypeTag[RDDNestedCollectionCompanionAbs]
    protected def getDefaultRep = RDDNestedCollection
  }
  implicit lazy val RDDNestedCollectionCompanionElem: RDDNestedCollectionCompanionElem = new RDDNestedCollectionCompanionElem

  implicit def proxyRDDNestedCollection[A](p: Rep[RDDNestedCollection[A]]): RDDNestedCollection[A] =
    proxyOps[RDDNestedCollection[A]](p)

  implicit class ExtendedRDDNestedCollection[A](p: Rep[RDDNestedCollection[A]])(implicit eA: Elem[A]) {
    def toData: Rep[RDDNestedCollectionData[A]] = isoRDDNestedCollection(eA).from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoRDDNestedCollection[A](implicit eA: Elem[A]): Iso[RDDNestedCollectionData[A], RDDNestedCollection[A]] =
    new RDDNestedCollectionIso[A]

  // 6) smart constructor and deconstructor
  def mkRDDNestedCollection[A](values: Rep[IRDDCollection[A]], segments: Rep[PairRDDCollection[Int,Int]])(implicit eA: Elem[A]): Rep[RDDNestedCollection[A]]
  def unmkRDDNestedCollection[A:Elem](p: Rep[RDDNestedCollection[A]]): Option[(Rep[IRDDCollection[A]], Rep[PairRDDCollection[Int,Int]])]
}

// Seq -----------------------------------
trait RDDCollectionsSeq extends RDDCollectionsDsl with SparkDslSeq {
  self: SparkDsl with RDDCollectionsDslSeq =>
  lazy val IRDDCollection: Rep[IRDDCollectionCompanionAbs] = new IRDDCollectionCompanionAbs with UserTypeSeq[IRDDCollectionCompanionAbs] {
    lazy val selfType = element[IRDDCollectionCompanionAbs]
  }

  case class SeqRDDCollection[A]
      (override val rdd: RepRDD[A])
      (implicit eA: Elem[A])
    extends RDDCollection[A](rdd)
        with UserTypeSeq[RDDCollection[A]] {
    lazy val selfType = element[RDDCollection[A]]
  }
  lazy val RDDCollection = new RDDCollectionCompanionAbs with UserTypeSeq[RDDCollectionCompanionAbs] {
    lazy val selfType = element[RDDCollectionCompanionAbs]
  }

  def mkRDDCollection[A]
      (rdd: RepRDD[A])(implicit eA: Elem[A]): Rep[RDDCollection[A]] =
      new SeqRDDCollection[A](rdd)
  def unmkRDDCollection[A:Elem](p: Rep[RDDCollection[A]]) =
    Some((p.rdd))

  case class SeqPairRDDCollection[A, B]
      (override val pairRDD: RepRDD[(A, B)])
      (implicit eA: Elem[A], eB: Elem[B])
    extends PairRDDCollection[A, B](pairRDD)
        with UserTypeSeq[PairRDDCollection[A, B]] {
    lazy val selfType = element[PairRDDCollection[A, B]]
  }
  lazy val PairRDDCollection = new PairRDDCollectionCompanionAbs with UserTypeSeq[PairRDDCollectionCompanionAbs] {
    lazy val selfType = element[PairRDDCollectionCompanionAbs]
  }

  def mkPairRDDCollection[A, B]
      (pairRDD: RepRDD[(A, B)])(implicit eA: Elem[A], eB: Elem[B]): Rep[PairRDDCollection[A, B]] =
      new SeqPairRDDCollection[A, B](pairRDD)
  def unmkPairRDDCollection[A:Elem, B:Elem](p: Rep[PairRDDCollection[A, B]]) =
    Some((p.pairRDD))

  case class SeqRDDNestedCollection[A]
      (override val values: Rep[IRDDCollection[A]], override val segments: Rep[PairRDDCollection[Int,Int]])
      (implicit eA: Elem[A])
    extends RDDNestedCollection[A](values, segments)
        with UserTypeSeq[RDDNestedCollection[A]] {
    lazy val selfType = element[RDDNestedCollection[A]]
  }
  lazy val RDDNestedCollection = new RDDNestedCollectionCompanionAbs with UserTypeSeq[RDDNestedCollectionCompanionAbs] {
    lazy val selfType = element[RDDNestedCollectionCompanionAbs]
  }

  def mkRDDNestedCollection[A]
      (values: Rep[IRDDCollection[A]], segments: Rep[PairRDDCollection[Int,Int]])(implicit eA: Elem[A]): Rep[RDDNestedCollection[A]] =
      new SeqRDDNestedCollection[A](values, segments)
  def unmkRDDNestedCollection[A:Elem](p: Rep[RDDNestedCollection[A]]) =
    Some((p.values, p.segments))
}

// Exp -----------------------------------
trait RDDCollectionsExp extends RDDCollectionsDsl with SparkDslExp {
  self: SparkDsl with RDDCollectionsDslExp =>
  lazy val IRDDCollection: Rep[IRDDCollectionCompanionAbs] = new IRDDCollectionCompanionAbs with UserTypeDef[IRDDCollectionCompanionAbs] {
    lazy val selfType = element[IRDDCollectionCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  case class ExpRDDCollection[A]
      (override val rdd: RepRDD[A])
      (implicit eA: Elem[A])
    extends RDDCollection[A](rdd) with UserTypeDef[RDDCollection[A]] {
    lazy val selfType = element[RDDCollection[A]]
    override def mirror(t: Transformer) = ExpRDDCollection[A](t(rdd))
  }

  lazy val RDDCollection: Rep[RDDCollectionCompanionAbs] = new RDDCollectionCompanionAbs with UserTypeDef[RDDCollectionCompanionAbs] {
    lazy val selfType = element[RDDCollectionCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object RDDCollectionMethods {
    object arr {
      def unapply(d: Def[_]): Option[Rep[RDDCollection[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[RDDCollectionElem[_]] && method.getName == "arr" =>
          Some(receiver).asInstanceOf[Option[Rep[RDDCollection[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[RDDCollection[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object lst {
      def unapply(d: Def[_]): Option[Rep[RDDCollection[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[RDDCollectionElem[_]] && method.getName == "lst" =>
          Some(receiver).asInstanceOf[Option[Rep[RDDCollection[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[RDDCollection[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object apply {
      def unapply(d: Def[_]): Option[(Rep[RDDCollection[A]], Rep[Int]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(i, _*), _) if receiver.elem.isInstanceOf[RDDCollectionElem[_]] && method.getName == "apply"&& method.getAnnotation(classOf[scalan.OverloadId]) == null =>
          Some((receiver, i)).asInstanceOf[Option[(Rep[RDDCollection[A]], Rep[Int]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDCollection[A]], Rep[Int]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object length {
      def unapply(d: Def[_]): Option[Rep[RDDCollection[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[RDDCollectionElem[_]] && method.getName == "length" =>
          Some(receiver).asInstanceOf[Option[Rep[RDDCollection[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[RDDCollection[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object slice {
      def unapply(d: Def[_]): Option[(Rep[RDDCollection[A]], Rep[Int], Rep[Int]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(offset, length, _*), _) if receiver.elem.isInstanceOf[RDDCollectionElem[_]] && method.getName == "slice" =>
          Some((receiver, offset, length)).asInstanceOf[Option[(Rep[RDDCollection[A]], Rep[Int], Rep[Int]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDCollection[A]], Rep[Int], Rep[Int]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object apply_many {
      def unapply(d: Def[_]): Option[(Rep[RDDCollection[A]], Coll[Int]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(indices, _*), _) if receiver.elem.isInstanceOf[RDDCollectionElem[_]] && method.getName == "apply" && { val ann = method.getAnnotation(classOf[scalan.OverloadId]); ann != null && ann.value == "many" } =>
          Some((receiver, indices)).asInstanceOf[Option[(Rep[RDDCollection[A]], Coll[Int]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDCollection[A]], Coll[Int]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object mapBy {
      def unapply(d: Def[_]): Option[(Rep[RDDCollection[A]], Rep[A => B]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[RDDCollectionElem[_]] && method.getName == "mapBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[RDDCollection[A]], Rep[A => B]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDCollection[A]], Rep[A => B]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object reduce {
      def unapply(d: Def[_]): Option[(Rep[RDDCollection[A]], RepMonoid[A]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(m, _*), _) if receiver.elem.isInstanceOf[RDDCollectionElem[_]] && method.getName == "reduce" =>
          Some((receiver, m)).asInstanceOf[Option[(Rep[RDDCollection[A]], RepMonoid[A]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDCollection[A]], RepMonoid[A]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object zip {
      def unapply(d: Def[_]): Option[(Rep[RDDCollection[A]], Coll[B]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(ys, _*), _) if receiver.elem.isInstanceOf[RDDCollectionElem[_]] && method.getName == "zip" =>
          Some((receiver, ys)).asInstanceOf[Option[(Rep[RDDCollection[A]], Coll[B]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDCollection[A]], Coll[B]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object update {
      def unapply(d: Def[_]): Option[(Rep[RDDCollection[A]], Rep[Int], Rep[A]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(idx, value, _*), _) if receiver.elem.isInstanceOf[RDDCollectionElem[_]] && method.getName == "update" =>
          Some((receiver, idx, value)).asInstanceOf[Option[(Rep[RDDCollection[A]], Rep[Int], Rep[A]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDCollection[A]], Rep[Int], Rep[A]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object updateMany {
      def unapply(d: Def[_]): Option[(Rep[RDDCollection[A]], Coll[Int], Coll[A]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(idxs, vals, _*), _) if receiver.elem.isInstanceOf[RDDCollectionElem[_]] && method.getName == "updateMany" =>
          Some((receiver, idxs, vals)).asInstanceOf[Option[(Rep[RDDCollection[A]], Coll[Int], Coll[A]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDCollection[A]], Coll[Int], Coll[A]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object filterBy {
      def unapply(d: Def[_]): Option[(Rep[RDDCollection[A]], Rep[A => Boolean]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[RDDCollectionElem[_]] && method.getName == "filterBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[RDDCollection[A]], Rep[A => Boolean]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDCollection[A]], Rep[A => Boolean]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object flatMapBy {
      def unapply(d: Def[_]): Option[(Rep[RDDCollection[A]], Rep[A => Collection[B]]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[RDDCollectionElem[_]] && method.getName == "flatMapBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[RDDCollection[A]], Rep[A => Collection[B]]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDCollection[A]], Rep[A => Collection[B]]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object append {
      def unapply(d: Def[_]): Option[(Rep[RDDCollection[A]], Rep[A]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(value, _*), _) if receiver.elem.isInstanceOf[RDDCollectionElem[_]] && method.getName == "append" =>
          Some((receiver, value)).asInstanceOf[Option[(Rep[RDDCollection[A]], Rep[A]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDCollection[A]], Rep[A]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object RDDCollectionCompanionMethods {
  }

  def mkRDDCollection[A]
    (rdd: RepRDD[A])(implicit eA: Elem[A]): Rep[RDDCollection[A]] =
    new ExpRDDCollection[A](rdd)
  def unmkRDDCollection[A:Elem](p: Rep[RDDCollection[A]]) =
    Some((p.rdd))

  case class ExpPairRDDCollection[A, B]
      (override val pairRDD: RepRDD[(A, B)])
      (implicit eA: Elem[A], eB: Elem[B])
    extends PairRDDCollection[A, B](pairRDD) with UserTypeDef[PairRDDCollection[A, B]] {
    lazy val selfType = element[PairRDDCollection[A, B]]
    override def mirror(t: Transformer) = ExpPairRDDCollection[A, B](t(pairRDD))
  }

  lazy val PairRDDCollection: Rep[PairRDDCollectionCompanionAbs] = new PairRDDCollectionCompanionAbs with UserTypeDef[PairRDDCollectionCompanionAbs] {
    lazy val selfType = element[PairRDDCollectionCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object PairRDDCollectionMethods {
    object as {
      def unapply(d: Def[_]): Option[Rep[PairRDDCollection[A, B]] forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[PairRDDCollectionElem[_, _]] && method.getName == "as" =>
          Some(receiver).asInstanceOf[Option[Rep[PairRDDCollection[A, B]] forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[PairRDDCollection[A, B]] forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object bs {
      def unapply(d: Def[_]): Option[Rep[PairRDDCollection[A, B]] forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[PairRDDCollectionElem[_, _]] && method.getName == "bs" =>
          Some(receiver).asInstanceOf[Option[Rep[PairRDDCollection[A, B]] forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[PairRDDCollection[A, B]] forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object arr {
      def unapply(d: Def[_]): Option[Rep[PairRDDCollection[A, B]] forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[PairRDDCollectionElem[_, _]] && method.getName == "arr" =>
          Some(receiver).asInstanceOf[Option[Rep[PairRDDCollection[A, B]] forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[PairRDDCollection[A, B]] forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object lst {
      def unapply(d: Def[_]): Option[Rep[PairRDDCollection[A, B]] forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[PairRDDCollectionElem[_, _]] && method.getName == "lst" =>
          Some(receiver).asInstanceOf[Option[Rep[PairRDDCollection[A, B]] forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[PairRDDCollection[A, B]] forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object apply {
      def unapply(d: Def[_]): Option[(Rep[PairRDDCollection[A, B]], Rep[Int]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(i, _*), _) if receiver.elem.isInstanceOf[PairRDDCollectionElem[_, _]] && method.getName == "apply"&& method.getAnnotation(classOf[scalan.OverloadId]) == null =>
          Some((receiver, i)).asInstanceOf[Option[(Rep[PairRDDCollection[A, B]], Rep[Int]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDCollection[A, B]], Rep[Int]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object length {
      def unapply(d: Def[_]): Option[Rep[PairRDDCollection[A, B]] forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[PairRDDCollectionElem[_, _]] && method.getName == "length" =>
          Some(receiver).asInstanceOf[Option[Rep[PairRDDCollection[A, B]] forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[PairRDDCollection[A, B]] forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object slice {
      def unapply(d: Def[_]): Option[(Rep[PairRDDCollection[A, B]], Rep[Int], Rep[Int]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(offset, length, _*), _) if receiver.elem.isInstanceOf[PairRDDCollectionElem[_, _]] && method.getName == "slice" =>
          Some((receiver, offset, length)).asInstanceOf[Option[(Rep[PairRDDCollection[A, B]], Rep[Int], Rep[Int]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDCollection[A, B]], Rep[Int], Rep[Int]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object apply_many {
      def unapply(d: Def[_]): Option[(Rep[PairRDDCollection[A, B]], Coll[Int]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(indices, _*), _) if receiver.elem.isInstanceOf[PairRDDCollectionElem[_, _]] && method.getName == "apply" && { val ann = method.getAnnotation(classOf[scalan.OverloadId]); ann != null && ann.value == "many" } =>
          Some((receiver, indices)).asInstanceOf[Option[(Rep[PairRDDCollection[A, B]], Coll[Int]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDCollection[A, B]], Coll[Int]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object mapBy {
      def unapply(d: Def[_]): Option[(Rep[PairRDDCollection[A, B]], Rep[((A, B)) => C]) forSome {type A; type B; type C}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[PairRDDCollectionElem[_, _]] && method.getName == "mapBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[PairRDDCollection[A, B]], Rep[((A, B)) => C]) forSome {type A; type B; type C}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDCollection[A, B]], Rep[((A, B)) => C]) forSome {type A; type B; type C}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object reduce {
      def unapply(d: Def[_]): Option[(Rep[PairRDDCollection[A, B]], RepMonoid[(A, B)]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(m, _*), _) if receiver.elem.isInstanceOf[PairRDDCollectionElem[_, _]] && method.getName == "reduce" =>
          Some((receiver, m)).asInstanceOf[Option[(Rep[PairRDDCollection[A, B]], RepMonoid[(A, B)]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDCollection[A, B]], RepMonoid[(A, B)]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object zip {
      def unapply(d: Def[_]): Option[(Rep[PairRDDCollection[A, B]], Coll[C]) forSome {type A; type B; type C}] = d match {
        case MethodCall(receiver, method, Seq(ys, _*), _) if receiver.elem.isInstanceOf[PairRDDCollectionElem[_, _]] && method.getName == "zip" =>
          Some((receiver, ys)).asInstanceOf[Option[(Rep[PairRDDCollection[A, B]], Coll[C]) forSome {type A; type B; type C}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDCollection[A, B]], Coll[C]) forSome {type A; type B; type C}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object update {
      def unapply(d: Def[_]): Option[(Rep[PairRDDCollection[A, B]], Rep[Int], Rep[(A, B)]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(idx, value, _*), _) if receiver.elem.isInstanceOf[PairRDDCollectionElem[_, _]] && method.getName == "update" =>
          Some((receiver, idx, value)).asInstanceOf[Option[(Rep[PairRDDCollection[A, B]], Rep[Int], Rep[(A, B)]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDCollection[A, B]], Rep[Int], Rep[(A, B)]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object updateMany {
      def unapply(d: Def[_]): Option[(Rep[PairRDDCollection[A, B]], Coll[Int], Coll[(A, B)]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(idxs, vals, _*), _) if receiver.elem.isInstanceOf[PairRDDCollectionElem[_, _]] && method.getName == "updateMany" =>
          Some((receiver, idxs, vals)).asInstanceOf[Option[(Rep[PairRDDCollection[A, B]], Coll[Int], Coll[(A, B)]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDCollection[A, B]], Coll[Int], Coll[(A, B)]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object filterBy {
      def unapply(d: Def[_]): Option[(Rep[PairRDDCollection[A, B]], Rep[((A, B)) => Boolean]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[PairRDDCollectionElem[_, _]] && method.getName == "filterBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[PairRDDCollection[A, B]], Rep[((A, B)) => Boolean]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDCollection[A, B]], Rep[((A, B)) => Boolean]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object flatMapBy {
      def unapply(d: Def[_]): Option[(Rep[PairRDDCollection[A, B]], Rep[((A, B)) => Collection[C]]) forSome {type A; type B; type C}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[PairRDDCollectionElem[_, _]] && method.getName == "flatMapBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[PairRDDCollection[A, B]], Rep[((A, B)) => Collection[C]]) forSome {type A; type B; type C}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDCollection[A, B]], Rep[((A, B)) => Collection[C]]) forSome {type A; type B; type C}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object append {
      def unapply(d: Def[_]): Option[(Rep[PairRDDCollection[A, B]], Rep[(A, B)]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(value, _*), _) if receiver.elem.isInstanceOf[PairRDDCollectionElem[_, _]] && method.getName == "append" =>
          Some((receiver, value)).asInstanceOf[Option[(Rep[PairRDDCollection[A, B]], Rep[(A, B)]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDCollection[A, B]], Rep[(A, B)]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object PairRDDCollectionCompanionMethods {
  }

  def mkPairRDDCollection[A, B]
    (pairRDD: RepRDD[(A, B)])(implicit eA: Elem[A], eB: Elem[B]): Rep[PairRDDCollection[A, B]] =
    new ExpPairRDDCollection[A, B](pairRDD)
  def unmkPairRDDCollection[A:Elem, B:Elem](p: Rep[PairRDDCollection[A, B]]) =
    Some((p.pairRDD))

  case class ExpRDDNestedCollection[A]
      (override val values: Rep[IRDDCollection[A]], override val segments: Rep[PairRDDCollection[Int,Int]])
      (implicit eA: Elem[A])
    extends RDDNestedCollection[A](values, segments) with UserTypeDef[RDDNestedCollection[A]] {
    lazy val selfType = element[RDDNestedCollection[A]]
    override def mirror(t: Transformer) = ExpRDDNestedCollection[A](t(values), t(segments))
  }

  lazy val RDDNestedCollection: Rep[RDDNestedCollectionCompanionAbs] = new RDDNestedCollectionCompanionAbs with UserTypeDef[RDDNestedCollectionCompanionAbs] {
    lazy val selfType = element[RDDNestedCollectionCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object RDDNestedCollectionMethods {
    object segOffsets {
      def unapply(d: Def[_]): Option[Rep[RDDNestedCollection[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[RDDNestedCollectionElem[_]] && method.getName == "segOffsets" =>
          Some(receiver).asInstanceOf[Option[Rep[RDDNestedCollection[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[RDDNestedCollection[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object segLens {
      def unapply(d: Def[_]): Option[Rep[RDDNestedCollection[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[RDDNestedCollectionElem[_]] && method.getName == "segLens" =>
          Some(receiver).asInstanceOf[Option[Rep[RDDNestedCollection[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[RDDNestedCollection[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object length {
      def unapply(d: Def[_]): Option[Rep[RDDNestedCollection[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[RDDNestedCollectionElem[_]] && method.getName == "length" =>
          Some(receiver).asInstanceOf[Option[Rep[RDDNestedCollection[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[RDDNestedCollection[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object apply {
      def unapply(d: Def[_]): Option[(Rep[RDDNestedCollection[A]], Rep[Int]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(i, _*), _) if receiver.elem.isInstanceOf[RDDNestedCollectionElem[_]] && method.getName == "apply"&& method.getAnnotation(classOf[scalan.OverloadId]) == null =>
          Some((receiver, i)).asInstanceOf[Option[(Rep[RDDNestedCollection[A]], Rep[Int]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDNestedCollection[A]], Rep[Int]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object arr {
      def unapply(d: Def[_]): Option[Rep[RDDNestedCollection[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[RDDNestedCollectionElem[_]] && method.getName == "arr" =>
          Some(receiver).asInstanceOf[Option[Rep[RDDNestedCollection[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[RDDNestedCollection[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object lst {
      def unapply(d: Def[_]): Option[Rep[RDDNestedCollection[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[RDDNestedCollectionElem[_]] && method.getName == "lst" =>
          Some(receiver).asInstanceOf[Option[Rep[RDDNestedCollection[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[RDDNestedCollection[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object slice {
      def unapply(d: Def[_]): Option[(Rep[RDDNestedCollection[A]], Rep[Int], Rep[Int]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(offset, length, _*), _) if receiver.elem.isInstanceOf[RDDNestedCollectionElem[_]] && method.getName == "slice" =>
          Some((receiver, offset, length)).asInstanceOf[Option[(Rep[RDDNestedCollection[A]], Rep[Int], Rep[Int]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDNestedCollection[A]], Rep[Int], Rep[Int]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object apply_many {
      def unapply(d: Def[_]): Option[(Rep[RDDNestedCollection[A]], Coll[Int]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(indices, _*), _) if receiver.elem.isInstanceOf[RDDNestedCollectionElem[_]] && method.getName == "apply" && { val ann = method.getAnnotation(classOf[scalan.OverloadId]); ann != null && ann.value == "many" } =>
          Some((receiver, indices)).asInstanceOf[Option[(Rep[RDDNestedCollection[A]], Coll[Int]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDNestedCollection[A]], Coll[Int]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object mapBy {
      def unapply(d: Def[_]): Option[(Rep[RDDNestedCollection[A]], Rep[Collection[A] => B]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[RDDNestedCollectionElem[_]] && method.getName == "mapBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[RDDNestedCollection[A]], Rep[Collection[A] => B]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDNestedCollection[A]], Rep[Collection[A] => B]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object reduce {
      def unapply(d: Def[_]): Option[(Rep[RDDNestedCollection[A]], RepMonoid[Collection[A]]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(m, _*), _) if receiver.elem.isInstanceOf[RDDNestedCollectionElem[_]] && method.getName == "reduce" =>
          Some((receiver, m)).asInstanceOf[Option[(Rep[RDDNestedCollection[A]], RepMonoid[Collection[A]]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDNestedCollection[A]], RepMonoid[Collection[A]]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object zip {
      def unapply(d: Def[_]): Option[(Rep[RDDNestedCollection[A]], Coll[B]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(ys, _*), _) if receiver.elem.isInstanceOf[RDDNestedCollectionElem[_]] && method.getName == "zip" =>
          Some((receiver, ys)).asInstanceOf[Option[(Rep[RDDNestedCollection[A]], Coll[B]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDNestedCollection[A]], Coll[B]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object update {
      def unapply(d: Def[_]): Option[(Rep[RDDNestedCollection[A]], Rep[Int], Rep[Collection[A]]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(idx, value, _*), _) if receiver.elem.isInstanceOf[RDDNestedCollectionElem[_]] && method.getName == "update" =>
          Some((receiver, idx, value)).asInstanceOf[Option[(Rep[RDDNestedCollection[A]], Rep[Int], Rep[Collection[A]]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDNestedCollection[A]], Rep[Int], Rep[Collection[A]]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object updateMany {
      def unapply(d: Def[_]): Option[(Rep[RDDNestedCollection[A]], Coll[Int], Coll[Collection[A]]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(idxs, vals, _*), _) if receiver.elem.isInstanceOf[RDDNestedCollectionElem[_]] && method.getName == "updateMany" =>
          Some((receiver, idxs, vals)).asInstanceOf[Option[(Rep[RDDNestedCollection[A]], Coll[Int], Coll[Collection[A]]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDNestedCollection[A]], Coll[Int], Coll[Collection[A]]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object filterBy {
      def unapply(d: Def[_]): Option[(Rep[RDDNestedCollection[A]], Rep[Collection[A] => Boolean]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[RDDNestedCollectionElem[_]] && method.getName == "filterBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[RDDNestedCollection[A]], Rep[Collection[A] => Boolean]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDNestedCollection[A]], Rep[Collection[A] => Boolean]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object flatMapBy {
      def unapply(d: Def[_]): Option[(Rep[RDDNestedCollection[A]], Rep[Collection[A] => Collection[B]]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[RDDNestedCollectionElem[_]] && method.getName == "flatMapBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[RDDNestedCollection[A]], Rep[Collection[A] => Collection[B]]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDNestedCollection[A]], Rep[Collection[A] => Collection[B]]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object append {
      def unapply(d: Def[_]): Option[(Rep[RDDNestedCollection[A]], Rep[Collection[A]]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(value, _*), _) if receiver.elem.isInstanceOf[RDDNestedCollectionElem[_]] && method.getName == "append" =>
          Some((receiver, value)).asInstanceOf[Option[(Rep[RDDNestedCollection[A]], Rep[Collection[A]]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDNestedCollection[A]], Rep[Collection[A]]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object RDDNestedCollectionCompanionMethods {
    object createRDDNestedCollection {
      def unapply(d: Def[_]): Option[(Rep[RDDCollection[A]], Rep[PairRDDCollection[Int,Int]]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(vals, segments, _*), _) if receiver.elem.isInstanceOf[RDDNestedCollectionCompanionElem] && method.getName == "createRDDNestedCollection" =>
          Some((vals, segments)).asInstanceOf[Option[(Rep[RDDCollection[A]], Rep[PairRDDCollection[Int,Int]]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDCollection[A]], Rep[PairRDDCollection[Int,Int]]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  def mkRDDNestedCollection[A]
    (values: Rep[IRDDCollection[A]], segments: Rep[PairRDDCollection[Int,Int]])(implicit eA: Elem[A]): Rep[RDDNestedCollection[A]] =
    new ExpRDDNestedCollection[A](values, segments)
  def unmkRDDNestedCollection[A:Elem](p: Rep[RDDNestedCollection[A]]) =
    Some((p.values, p.segments))

  object IRDDCollectionMethods {
    object rdd {
      def unapply(d: Def[_]): Option[Rep[IRDDCollection[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[IRDDCollectionElem[_, _]] && method.getName == "rdd" =>
          Some(receiver).asInstanceOf[Option[Rep[IRDDCollection[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[IRDDCollection[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object IRDDCollectionCompanionMethods {
  }
}
