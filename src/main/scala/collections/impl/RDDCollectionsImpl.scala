package scalan.spark.collections
package impl

import scala.annotation.unchecked.uncheckedVariance
import scalan.OverloadId
import scalan.common.Default
import scalan.common.OverloadHack.Overloaded1
import scalan.spark.{SparkDslExp, SparkDslSeq, SparkDsl}
import scala.reflect._
import scala.reflect.runtime.universe._

// Abs -----------------------------------
trait RDDCollectionsAbs extends RDDCollections with SparkDsl {
  self: SparkDsl with RDDCollectionsDsl =>

  // single proxy for each type family
  implicit def proxyIRDDCollection[A](p: Rep[IRDDCollection[A]]): IRDDCollection[A] = {
    proxyOps[IRDDCollection[A]](p)(scala.reflect.classTag[IRDDCollection[A]])
  }

  // familyElem
  class IRDDCollectionElem[A, To <: IRDDCollection[A]](implicit override val eItem: Elem[A])
    extends CollectionElem[A, To] {
    override def isEntityType = true
    override lazy val tag = {
      implicit val tagA = eItem.tag
      weakTypeTag[IRDDCollection[A]].asInstanceOf[WeakTypeTag[To]]
    }
    override def convert(x: Rep[Reifiable[_]]) = {
      implicit val eTo: Elem[To] = this
      val conv = fun {x: Rep[IRDDCollection[A]] => convertIRDDCollection(x) }
      tryConvert(element[IRDDCollection[A]], this, x, conv)
    }

    def convertIRDDCollection(x : Rep[IRDDCollection[A]]): Rep[To] = {
      assert(x.selfType1 match { case _: IRDDCollectionElem[_, _] => true; case _ => false })
      x.asRep[To]
    }
    override def getDefaultRep: Rep[To] = ???
  }

  implicit def iRDDCollectionElement[A](implicit eItem: Elem[A]): Elem[IRDDCollection[A]] =
    new IRDDCollectionElem[A, IRDDCollection[A]]

  implicit case object IRDDCollectionCompanionElem extends CompanionElem[IRDDCollectionCompanionAbs] {
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
    proxyOps[IRDDPairCollection[A, B]](p)(scala.reflect.classTag[IRDDPairCollection[A, B]])
  }
  // familyElem
  class IRDDPairCollectionElem[A, B, To <: IRDDPairCollection[A, B]](implicit override val eA: Elem[A], override val eB: Elem[B])
    extends PairCollectionElem[A, B, To] {
    override def isEntityType = true
    override lazy val tag = {
      implicit val tagA = eItem.tag
      implicit val tagB = eB.tag
      weakTypeTag[IRDDPairCollection[A, B]].asInstanceOf[WeakTypeTag[To]]
    }
    override def convert(x: Rep[Reifiable[_]]) = {
      implicit val eTo: Elem[To] = this
      val conv = fun {x: Rep[IRDDPairCollection[A, B]] => convertIRDDPairCollection(x) }
      tryConvert(element[IRDDPairCollection[A, B]], this, x, conv)
    }

    def convertIRDDPairCollection(x : Rep[IRDDPairCollection[A, B]]): Rep[To] = {
      assert(x.selfType1 match { case _: IRDDPairCollectionElem[_, _, _] => true; case _ => false })
      x.asRep[To]
    }
    override def getDefaultRep: Rep[To] = ???
  }

  implicit def iRDDPairCollectionElement[A, B](implicit eA: Elem[A], eB: Elem[B]): Elem[IRDDPairCollection[A, B]] =
    new IRDDPairCollectionElem[A, B, IRDDPairCollection[A, B]]

  // single proxy for each type family
  implicit def proxyIRDDNestedCollection[A](p: Rep[IRDDNestedCollection[A]]): IRDDNestedCollection[A] = {
    proxyOps[IRDDNestedCollection[A]](p)(scala.reflect.classTag[IRDDNestedCollection[A]])
  }
  // familyElem
  class IRDDNestedCollectionElem[A, To <: IRDDNestedCollection[A]](implicit override val eA: Elem[A])
    extends NestedCollectionElem[A, To] {
    override def isEntityType = true
    override lazy val tag = {
      implicit val tagA = eA.tag
      weakTypeTag[IRDDNestedCollection[A]].asInstanceOf[WeakTypeTag[To]]
    }
    override def convert(x: Rep[Reifiable[_]]) = {
      implicit val eTo: Elem[To] = this
      val conv = fun {x: Rep[IRDDNestedCollection[A]] => convertIRDDNestedCollection(x) }
      tryConvert(element[IRDDNestedCollection[A]], this, x, conv)
    }

    def convertIRDDNestedCollection(x : Rep[IRDDNestedCollection[A]]): Rep[To] = {
      assert(x.selfType1 match { case _: IRDDNestedCollectionElem[_, _] => true; case _ => false })
      x.asRep[To]
    }
    override def getDefaultRep: Rep[To] = ???
  }

  implicit def iRDDNestedCollectionElement[A](implicit eA: Elem[A]): Elem[IRDDNestedCollection[A]] =
    new IRDDNestedCollectionElem[A, IRDDNestedCollection[A]]

  // elem for concrete class
  class RDDCollectionElem[A](val iso: Iso[RDDCollectionData[A], RDDCollection[A]])(implicit eItem: Elem[A])
    extends IRDDCollectionElem[A, RDDCollection[A]]
    with ConcreteElem[RDDCollectionData[A], RDDCollection[A]] {
    override def convertIRDDCollection(x: Rep[IRDDCollection[A]]) = RDDCollection(x.rdd)
    override def getDefaultRep = super[ConcreteElem].getDefaultRep
    override lazy val tag = {
      implicit val tagA = eItem.tag
      weakTypeTag[RDDCollection[A]]
    }
  }

  // state representation type
  type RDDCollectionData[A] = SRDD[A]

  // 3) Iso for concrete class
  class RDDCollectionIso[A](implicit eItem: Elem[A])
    extends Iso[RDDCollectionData[A], RDDCollection[A]] {
    override def from(p: Rep[RDDCollection[A]]) =
      p.rdd
    override def to(p: Rep[SRDD[A]]) = {
      val rdd = p
      RDDCollection(rdd)
    }
    lazy val defaultRepTo: Rep[RDDCollection[A]] = RDDCollection(element[SRDD[A]].defaultRepValue)
    lazy val eTo = new RDDCollectionElem[A](this)
  }
  // 4) constructor and deconstructor
  abstract class RDDCollectionCompanionAbs extends CompanionBase[RDDCollectionCompanionAbs] with RDDCollectionCompanion {
    override def toString = "RDDCollection"

    def apply[A](rdd: RepRDD[A])(implicit eItem: Elem[A]): Rep[RDDCollection[A]] =
      mkRDDCollection(rdd)
  }
  object RDDCollectionMatcher {
    def unapply[A](p: Rep[IRDDCollection[A]]) = unmkRDDCollection(p)
  }
  def RDDCollection: Rep[RDDCollectionCompanionAbs]
  implicit def proxyRDDCollectionCompanion(p: Rep[RDDCollectionCompanionAbs]): RDDCollectionCompanionAbs = {
    proxyOps[RDDCollectionCompanionAbs](p)
  }

  implicit case object RDDCollectionCompanionElem extends CompanionElem[RDDCollectionCompanionAbs] {
    lazy val tag = weakTypeTag[RDDCollectionCompanionAbs]
    protected def getDefaultRep = RDDCollection
  }

  implicit def proxyRDDCollection[A](p: Rep[RDDCollection[A]]): RDDCollection[A] =
    proxyOps[RDDCollection[A]](p)

  implicit class ExtendedRDDCollection[A](p: Rep[RDDCollection[A]])(implicit eItem: Elem[A]) {
    def toData: Rep[RDDCollectionData[A]] = isoRDDCollection(eItem).from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoRDDCollection[A](implicit eItem: Elem[A]): Iso[RDDCollectionData[A], RDDCollection[A]] =
    new RDDCollectionIso[A]

  // 6) smart constructor and deconstructor
  def mkRDDCollection[A](rdd: RepRDD[A])(implicit eItem: Elem[A]): Rep[RDDCollection[A]]
  def unmkRDDCollection[A](p: Rep[IRDDCollection[A]]): Option[(Rep[SRDD[A]])]

  // elem for concrete class
  class RDDIndexedCollectionElem[A](val iso: Iso[RDDIndexedCollectionData[A], RDDIndexedCollection[A]])(implicit eItem: Elem[A])
    extends IRDDCollectionElem[A, RDDIndexedCollection[A]]
    with ConcreteElem[RDDIndexedCollectionData[A], RDDIndexedCollection[A]] {
    override def convertIRDDCollection(x: Rep[IRDDCollection[A]]) = // Converter is not generated by meta
!!!("Cannot convert from IRDDCollection to RDDIndexedCollection: missing fields List(indexedRdd)")
    override def getDefaultRep = super[ConcreteElem].getDefaultRep
    override lazy val tag = {
      implicit val tagA = eItem.tag
      weakTypeTag[RDDIndexedCollection[A]]
    }
  }

  // state representation type
  type RDDIndexedCollectionData[A] = SRDD[(Long, A)]

  // 3) Iso for concrete class
  class RDDIndexedCollectionIso[A](implicit eItem: Elem[A])
    extends Iso[RDDIndexedCollectionData[A], RDDIndexedCollection[A]] {
    override def from(p: Rep[RDDIndexedCollection[A]]) =
      p.indexedRdd
    override def to(p: Rep[SRDD[(Long, A)]]) = {
      val indexedRdd = p
      RDDIndexedCollection(indexedRdd)
    }
    lazy val defaultRepTo: Rep[RDDIndexedCollection[A]] = RDDIndexedCollection(element[SRDD[(Long, A)]].defaultRepValue)
    lazy val eTo = new RDDIndexedCollectionElem[A](this)
  }
  // 4) constructor and deconstructor
  abstract class RDDIndexedCollectionCompanionAbs extends CompanionBase[RDDIndexedCollectionCompanionAbs] with RDDIndexedCollectionCompanion {
    override def toString = "RDDIndexedCollection"

    def apply[A](indexedRdd: RepRDD[(Long, A)])(implicit eItem: Elem[A]): Rep[RDDIndexedCollection[A]] =
      mkRDDIndexedCollection(indexedRdd)
  }
  object RDDIndexedCollectionMatcher {
    def unapply[A](p: Rep[IRDDCollection[A]]) = unmkRDDIndexedCollection(p)
  }
  def RDDIndexedCollection: Rep[RDDIndexedCollectionCompanionAbs]
  implicit def proxyRDDIndexedCollectionCompanion(p: Rep[RDDIndexedCollectionCompanionAbs]): RDDIndexedCollectionCompanionAbs = {
    proxyOps[RDDIndexedCollectionCompanionAbs](p)
  }

  implicit case object RDDIndexedCollectionCompanionElem extends CompanionElem[RDDIndexedCollectionCompanionAbs] {
    lazy val tag = weakTypeTag[RDDIndexedCollectionCompanionAbs]
    protected def getDefaultRep = RDDIndexedCollection
  }

  implicit def proxyRDDIndexedCollection[A](p: Rep[RDDIndexedCollection[A]]): RDDIndexedCollection[A] =
    proxyOps[RDDIndexedCollection[A]](p)

  implicit class ExtendedRDDIndexedCollection[A](p: Rep[RDDIndexedCollection[A]])(implicit eItem: Elem[A]) {
    def toData: Rep[RDDIndexedCollectionData[A]] = isoRDDIndexedCollection(eItem).from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoRDDIndexedCollection[A](implicit eItem: Elem[A]): Iso[RDDIndexedCollectionData[A], RDDIndexedCollection[A]] =
    new RDDIndexedCollectionIso[A]

  // 6) smart constructor and deconstructor
  def mkRDDIndexedCollection[A](indexedRdd: RepRDD[(Long, A)])(implicit eItem: Elem[A]): Rep[RDDIndexedCollection[A]]
  def unmkRDDIndexedCollection[A](p: Rep[IRDDCollection[A]]): Option[(Rep[SRDD[(Long, A)]])]

  // elem for concrete class
  class PairRDDCollectionElem[A, B](val iso: Iso[PairRDDCollectionData[A, B], PairRDDCollection[A, B]])(implicit eA: Elem[A], eB: Elem[B])
    extends IRDDPairCollectionElem[A, B, PairRDDCollection[A, B]]
    with ConcreteElem[PairRDDCollectionData[A, B], PairRDDCollection[A, B]] {
    override def convertIRDDPairCollection(x: Rep[IRDDPairCollection[A, B]]) = PairRDDCollection(x.pairRDD)
    override def getDefaultRep = super[ConcreteElem].getDefaultRep
    override lazy val tag = {
      implicit val tagA = eA.tag
      implicit val tagB = eB.tag
      weakTypeTag[PairRDDCollection[A, B]]
    }
  }

  // state representation type
  type PairRDDCollectionData[A, B] = SRDD[(A, B)]

  // 3) Iso for concrete class
  class PairRDDCollectionIso[A, B](implicit eA: Elem[A], eB: Elem[B])
    extends Iso[PairRDDCollectionData[A, B], PairRDDCollection[A, B]] {
    override def from(p: Rep[PairRDDCollection[A, B]]) =
      p.pairRDD
    override def to(p: Rep[SRDD[(A, B)]]) = {
      val pairRDD = p
      PairRDDCollection(pairRDD)
    }
    lazy val defaultRepTo: Rep[PairRDDCollection[A, B]] = PairRDDCollection(element[SRDD[(A, B)]].defaultRepValue)
    lazy val eTo = new PairRDDCollectionElem[A, B](this)
  }
  // 4) constructor and deconstructor
  abstract class PairRDDCollectionCompanionAbs extends CompanionBase[PairRDDCollectionCompanionAbs] with PairRDDCollectionCompanion {
    override def toString = "PairRDDCollection"

    def apply[A, B](pairRDD: RepRDD[(A, B)])(implicit eA: Elem[A], eB: Elem[B]): Rep[PairRDDCollection[A, B]] =
      mkPairRDDCollection(pairRDD)
  }
  object PairRDDCollectionMatcher {
    def unapply[A, B](p: Rep[IRDDPairCollection[A, B]]) = unmkPairRDDCollection(p)
  }
  def PairRDDCollection: Rep[PairRDDCollectionCompanionAbs]
  implicit def proxyPairRDDCollectionCompanion(p: Rep[PairRDDCollectionCompanionAbs]): PairRDDCollectionCompanionAbs = {
    proxyOps[PairRDDCollectionCompanionAbs](p)
  }

  implicit case object PairRDDCollectionCompanionElem extends CompanionElem[PairRDDCollectionCompanionAbs] {
    lazy val tag = weakTypeTag[PairRDDCollectionCompanionAbs]
    protected def getDefaultRep = PairRDDCollection
  }

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
  def unmkPairRDDCollection[A, B](p: Rep[IRDDPairCollection[A, B]]): Option[(Rep[SRDD[(A, B)]])]

  // elem for concrete class
  class PairRDDIndexedCollectionElem[A, B](val iso: Iso[PairRDDIndexedCollectionData[A, B], PairRDDIndexedCollection[A, B]])(implicit eA: Elem[A], eB: Elem[B])
    extends IRDDPairCollectionElem[A, B, PairRDDIndexedCollection[A, B]]
    with ConcreteElem[PairRDDIndexedCollectionData[A, B], PairRDDIndexedCollection[A, B]] {
    override def convertIRDDPairCollection(x: Rep[IRDDPairCollection[A, B]]) = // Converter is not generated by meta
!!!("Cannot convert from IRDDPairCollection to PairRDDIndexedCollection: missing fields List(indices)")
    override def getDefaultRep = super[ConcreteElem].getDefaultRep
    override lazy val tag = {
      implicit val tagA = eA.tag
      implicit val tagB = eB.tag
      weakTypeTag[PairRDDIndexedCollection[A, B]]
    }
  }

  // state representation type
  type PairRDDIndexedCollectionData[A, B] = (SRDD[Long], SRDD[(A, B)])

  // 3) Iso for concrete class
  class PairRDDIndexedCollectionIso[A, B](implicit eA: Elem[A], eB: Elem[B])
    extends Iso[PairRDDIndexedCollectionData[A, B], PairRDDIndexedCollection[A, B]]()(pairElement(implicitly[Elem[SRDD[Long]]], implicitly[Elem[SRDD[(A, B)]]])) {
    override def from(p: Rep[PairRDDIndexedCollection[A, B]]) =
      (p.indices, p.pairRDD)
    override def to(p: Rep[(SRDD[Long], SRDD[(A, B)])]) = {
      val Pair(indices, pairRDD) = p
      PairRDDIndexedCollection(indices, pairRDD)
    }
    lazy val defaultRepTo: Rep[PairRDDIndexedCollection[A, B]] = PairRDDIndexedCollection(element[SRDD[Long]].defaultRepValue, element[SRDD[(A, B)]].defaultRepValue)
    lazy val eTo = new PairRDDIndexedCollectionElem[A, B](this)
  }
  // 4) constructor and deconstructor
  abstract class PairRDDIndexedCollectionCompanionAbs extends CompanionBase[PairRDDIndexedCollectionCompanionAbs] with PairRDDIndexedCollectionCompanion {
    override def toString = "PairRDDIndexedCollection"
    def apply[A, B](p: Rep[PairRDDIndexedCollectionData[A, B]])(implicit eA: Elem[A], eB: Elem[B]): Rep[PairRDDIndexedCollection[A, B]] =
      isoPairRDDIndexedCollection(eA, eB).to(p)
    def apply[A, B](indices: RepRDD[Long], pairRDD: RepRDD[(A, B)])(implicit eA: Elem[A], eB: Elem[B]): Rep[PairRDDIndexedCollection[A, B]] =
      mkPairRDDIndexedCollection(indices, pairRDD)
  }
  object PairRDDIndexedCollectionMatcher {
    def unapply[A, B](p: Rep[IRDDPairCollection[A, B]]) = unmkPairRDDIndexedCollection(p)
  }
  def PairRDDIndexedCollection: Rep[PairRDDIndexedCollectionCompanionAbs]
  implicit def proxyPairRDDIndexedCollectionCompanion(p: Rep[PairRDDIndexedCollectionCompanionAbs]): PairRDDIndexedCollectionCompanionAbs = {
    proxyOps[PairRDDIndexedCollectionCompanionAbs](p)
  }

  implicit case object PairRDDIndexedCollectionCompanionElem extends CompanionElem[PairRDDIndexedCollectionCompanionAbs] {
    lazy val tag = weakTypeTag[PairRDDIndexedCollectionCompanionAbs]
    protected def getDefaultRep = PairRDDIndexedCollection
  }

  implicit def proxyPairRDDIndexedCollection[A, B](p: Rep[PairRDDIndexedCollection[A, B]]): PairRDDIndexedCollection[A, B] =
    proxyOps[PairRDDIndexedCollection[A, B]](p)

  implicit class ExtendedPairRDDIndexedCollection[A, B](p: Rep[PairRDDIndexedCollection[A, B]])(implicit eA: Elem[A], eB: Elem[B]) {
    def toData: Rep[PairRDDIndexedCollectionData[A, B]] = isoPairRDDIndexedCollection(eA, eB).from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoPairRDDIndexedCollection[A, B](implicit eA: Elem[A], eB: Elem[B]): Iso[PairRDDIndexedCollectionData[A, B], PairRDDIndexedCollection[A, B]] =
    new PairRDDIndexedCollectionIso[A, B]

  // 6) smart constructor and deconstructor
  def mkPairRDDIndexedCollection[A, B](indices: RepRDD[Long], pairRDD: RepRDD[(A, B)])(implicit eA: Elem[A], eB: Elem[B]): Rep[PairRDDIndexedCollection[A, B]]
  def unmkPairRDDIndexedCollection[A, B](p: Rep[IRDDPairCollection[A, B]]): Option[(Rep[SRDD[Long]], Rep[SRDD[(A, B)]])]

  // elem for concrete class
  class RDDNestedCollectionElem[A](val iso: Iso[RDDNestedCollectionData[A], RDDNestedCollection[A]])(implicit eA: Elem[A])
    extends IRDDNestedCollectionElem[A, RDDNestedCollection[A]]
    with ConcreteElem[RDDNestedCollectionData[A], RDDNestedCollection[A]] {
    override def convertIRDDNestedCollection(x: Rep[IRDDNestedCollection[A]]) = RDDNestedCollection(x.values, x.segments)
    override def getDefaultRep = super[ConcreteElem].getDefaultRep
    override lazy val tag = {
      implicit val tagA = eA.tag
      weakTypeTag[RDDNestedCollection[A]]
    }
  }

  // state representation type
  type RDDNestedCollectionData[A] = (IRDDCollection[A], PairRDDCollection[Int,Int])

  // 3) Iso for concrete class
  class RDDNestedCollectionIso[A](implicit eA: Elem[A])
    extends Iso[RDDNestedCollectionData[A], RDDNestedCollection[A]]()(pairElement(implicitly[Elem[IRDDCollection[A]]], implicitly[Elem[PairRDDCollection[Int,Int]]])) {
    override def from(p: Rep[RDDNestedCollection[A]]) =
      (p.values, p.segments)
    override def to(p: Rep[(IRDDCollection[A], PairRDDCollection[Int,Int])]) = {
      val Pair(values, segments) = p
      RDDNestedCollection(values, segments)
    }
    lazy val defaultRepTo: Rep[RDDNestedCollection[A]] = RDDNestedCollection(element[IRDDCollection[A]].defaultRepValue, element[PairRDDCollection[Int,Int]].defaultRepValue)
    lazy val eTo = new RDDNestedCollectionElem[A](this)
  }
  // 4) constructor and deconstructor
  abstract class RDDNestedCollectionCompanionAbs extends CompanionBase[RDDNestedCollectionCompanionAbs] with RDDNestedCollectionCompanion {
    override def toString = "RDDNestedCollection"
    def apply[A](p: Rep[RDDNestedCollectionData[A]])(implicit eA: Elem[A]): Rep[RDDNestedCollection[A]] =
      isoRDDNestedCollection(eA).to(p)
    def apply[A](values: Rep[IRDDCollection[A]], segments: Rep[PairRDDCollection[Int,Int]])(implicit eA: Elem[A]): Rep[RDDNestedCollection[A]] =
      mkRDDNestedCollection(values, segments)
  }
  object RDDNestedCollectionMatcher {
    def unapply[A](p: Rep[IRDDNestedCollection[A]]) = unmkRDDNestedCollection(p)
  }
  def RDDNestedCollection: Rep[RDDNestedCollectionCompanionAbs]
  implicit def proxyRDDNestedCollectionCompanion(p: Rep[RDDNestedCollectionCompanionAbs]): RDDNestedCollectionCompanionAbs = {
    proxyOps[RDDNestedCollectionCompanionAbs](p)
  }

  implicit case object RDDNestedCollectionCompanionElem extends CompanionElem[RDDNestedCollectionCompanionAbs] {
    lazy val tag = weakTypeTag[RDDNestedCollectionCompanionAbs]
    protected def getDefaultRep = RDDNestedCollection
  }

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
  def unmkRDDNestedCollection[A](p: Rep[IRDDNestedCollection[A]]): Option[(Rep[IRDDCollection[A]], Rep[PairRDDCollection[Int,Int]])]
}

// Seq -----------------------------------
trait RDDCollectionsSeq extends RDDCollectionsDsl with SparkDslSeq {
  self: SparkDsl with RDDCollectionsDslSeq =>
  lazy val IRDDCollection: Rep[IRDDCollectionCompanionAbs] = new IRDDCollectionCompanionAbs with UserTypeSeq[IRDDCollectionCompanionAbs] {
    lazy val selfType = element[IRDDCollectionCompanionAbs]
  }

  case class SeqRDDCollection[A]
      (override val rdd: RepRDD[A])
      (implicit eItem: Elem[A])
    extends RDDCollection[A](rdd)
        with UserTypeSeq[RDDCollection[A]] {
    lazy val selfType = element[RDDCollection[A]]
  }
  lazy val RDDCollection = new RDDCollectionCompanionAbs with UserTypeSeq[RDDCollectionCompanionAbs] {
    lazy val selfType = element[RDDCollectionCompanionAbs]
  }

  def mkRDDCollection[A]
      (rdd: RepRDD[A])(implicit eItem: Elem[A]): Rep[RDDCollection[A]] =
      new SeqRDDCollection[A](rdd)
  def unmkRDDCollection[A](p: Rep[IRDDCollection[A]]) = p match {
    case p: RDDCollection[A] @unchecked =>
      Some((p.rdd))
    case _ => None
  }

  case class SeqRDDIndexedCollection[A]
      (override val indexedRdd: RepRDD[(Long, A)])
      (implicit eItem: Elem[A])
    extends RDDIndexedCollection[A](indexedRdd)
        with UserTypeSeq[RDDIndexedCollection[A]] {
    lazy val selfType = element[RDDIndexedCollection[A]]
  }
  lazy val RDDIndexedCollection = new RDDIndexedCollectionCompanionAbs with UserTypeSeq[RDDIndexedCollectionCompanionAbs] {
    lazy val selfType = element[RDDIndexedCollectionCompanionAbs]
  }

  def mkRDDIndexedCollection[A]
      (indexedRdd: RepRDD[(Long, A)])(implicit eItem: Elem[A]): Rep[RDDIndexedCollection[A]] =
      new SeqRDDIndexedCollection[A](indexedRdd)
  def unmkRDDIndexedCollection[A](p: Rep[IRDDCollection[A]]) = p match {
    case p: RDDIndexedCollection[A] @unchecked =>
      Some((p.indexedRdd))
    case _ => None
  }

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
  def unmkPairRDDCollection[A, B](p: Rep[IRDDPairCollection[A, B]]) = p match {
    case p: PairRDDCollection[A, B] @unchecked =>
      Some((p.pairRDD))
    case _ => None
  }

  case class SeqPairRDDIndexedCollection[A, B]
      (override val indices: RepRDD[Long], override val pairRDD: RepRDD[(A, B)])
      (implicit eA: Elem[A], eB: Elem[B])
    extends PairRDDIndexedCollection[A, B](indices, pairRDD)
        with UserTypeSeq[PairRDDIndexedCollection[A, B]] {
    lazy val selfType = element[PairRDDIndexedCollection[A, B]]
  }
  lazy val PairRDDIndexedCollection = new PairRDDIndexedCollectionCompanionAbs with UserTypeSeq[PairRDDIndexedCollectionCompanionAbs] {
    lazy val selfType = element[PairRDDIndexedCollectionCompanionAbs]
  }

  def mkPairRDDIndexedCollection[A, B]
      (indices: RepRDD[Long], pairRDD: RepRDD[(A, B)])(implicit eA: Elem[A], eB: Elem[B]): Rep[PairRDDIndexedCollection[A, B]] =
      new SeqPairRDDIndexedCollection[A, B](indices, pairRDD)
  def unmkPairRDDIndexedCollection[A, B](p: Rep[IRDDPairCollection[A, B]]) = p match {
    case p: PairRDDIndexedCollection[A, B] @unchecked =>
      Some((p.indices, p.pairRDD))
    case _ => None
  }

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
  def unmkRDDNestedCollection[A](p: Rep[IRDDNestedCollection[A]]) = p match {
    case p: RDDNestedCollection[A] @unchecked =>
      Some((p.values, p.segments))
    case _ => None
  }
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
      (implicit eItem: Elem[A])
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
        case MethodCall(receiver, method, Seq(i, _*), _) if receiver.elem.isInstanceOf[RDDCollectionElem[_]] && method.getName == "apply" && method.getAnnotation(classOf[scalan.OverloadId]) == null =>
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
      def unapply(d: Def[_]): Option[(Rep[RDDCollection[A]], Rep[A @uncheckedVariance => B]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[RDDCollectionElem[_]] && method.getName == "mapBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[RDDCollection[A]], Rep[A @uncheckedVariance => B]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDCollection[A]], Rep[A @uncheckedVariance => B]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object reduce {
      def unapply(d: Def[_]): Option[(Rep[RDDCollection[A]], RepMonoid[A @uncheckedVariance]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(m, _*), _) if receiver.elem.isInstanceOf[RDDCollectionElem[_]] && method.getName == "reduce" =>
          Some((receiver, m)).asInstanceOf[Option[(Rep[RDDCollection[A]], RepMonoid[A @uncheckedVariance]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDCollection[A]], RepMonoid[A @uncheckedVariance]) forSome {type A}] = exp match {
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
      def unapply(d: Def[_]): Option[(Rep[RDDCollection[A]], Rep[A @uncheckedVariance => Boolean]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[RDDCollectionElem[_]] && method.getName == "filterBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[RDDCollection[A]], Rep[A @uncheckedVariance => Boolean]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDCollection[A]], Rep[A @uncheckedVariance => Boolean]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object flatMapBy {
      def unapply(d: Def[_]): Option[(Rep[RDDCollection[A]], Rep[A @uncheckedVariance => Collection[B]]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[RDDCollectionElem[_]] && method.getName == "flatMapBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[RDDCollection[A]], Rep[A @uncheckedVariance => Collection[B]]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDCollection[A]], Rep[A @uncheckedVariance => Collection[B]]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object append {
      def unapply(d: Def[_]): Option[(Rep[RDDCollection[A]], Rep[A @uncheckedVariance]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(value, _*), _) if receiver.elem.isInstanceOf[RDDCollectionElem[_]] && method.getName == "append" =>
          Some((receiver, value)).asInstanceOf[Option[(Rep[RDDCollection[A]], Rep[A @uncheckedVariance]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDCollection[A]], Rep[A @uncheckedVariance]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object RDDCollectionCompanionMethods {
  }

  def mkRDDCollection[A]
    (rdd: RepRDD[A])(implicit eItem: Elem[A]): Rep[RDDCollection[A]] =
    new ExpRDDCollection[A](rdd)
  def unmkRDDCollection[A](p: Rep[IRDDCollection[A]]) = p.elem.asInstanceOf[Elem[_]] match {
    case _: RDDCollectionElem[A] @unchecked =>
      Some((p.asRep[RDDCollection[A]].rdd))
    case _ =>
      None
  }

  case class ExpRDDIndexedCollection[A]
      (override val indexedRdd: RepRDD[(Long, A)])
      (implicit eItem: Elem[A])
    extends RDDIndexedCollection[A](indexedRdd) with UserTypeDef[RDDIndexedCollection[A]] {
    lazy val selfType = element[RDDIndexedCollection[A]]
    override def mirror(t: Transformer) = ExpRDDIndexedCollection[A](t(indexedRdd))
  }

  lazy val RDDIndexedCollection: Rep[RDDIndexedCollectionCompanionAbs] = new RDDIndexedCollectionCompanionAbs with UserTypeDef[RDDIndexedCollectionCompanionAbs] {
    lazy val selfType = element[RDDIndexedCollectionCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object RDDIndexedCollectionMethods {
    object indices {
      def unapply(d: Def[_]): Option[Rep[RDDIndexedCollection[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[RDDIndexedCollectionElem[_]] && method.getName == "indices" =>
          Some(receiver).asInstanceOf[Option[Rep[RDDIndexedCollection[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[RDDIndexedCollection[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object rdd {
      def unapply(d: Def[_]): Option[Rep[RDDIndexedCollection[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[RDDIndexedCollectionElem[_]] && method.getName == "rdd" =>
          Some(receiver).asInstanceOf[Option[Rep[RDDIndexedCollection[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[RDDIndexedCollection[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object arr {
      def unapply(d: Def[_]): Option[Rep[RDDIndexedCollection[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[RDDIndexedCollectionElem[_]] && method.getName == "arr" =>
          Some(receiver).asInstanceOf[Option[Rep[RDDIndexedCollection[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[RDDIndexedCollection[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object lst {
      def unapply(d: Def[_]): Option[Rep[RDDIndexedCollection[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[RDDIndexedCollectionElem[_]] && method.getName == "lst" =>
          Some(receiver).asInstanceOf[Option[Rep[RDDIndexedCollection[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[RDDIndexedCollection[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object apply {
      def unapply(d: Def[_]): Option[(Rep[RDDIndexedCollection[A]], Rep[Int]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(i, _*), _) if receiver.elem.isInstanceOf[RDDIndexedCollectionElem[_]] && method.getName == "apply" && method.getAnnotation(classOf[scalan.OverloadId]) == null =>
          Some((receiver, i)).asInstanceOf[Option[(Rep[RDDIndexedCollection[A]], Rep[Int]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDIndexedCollection[A]], Rep[Int]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object length {
      def unapply(d: Def[_]): Option[Rep[RDDIndexedCollection[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[RDDIndexedCollectionElem[_]] && method.getName == "length" =>
          Some(receiver).asInstanceOf[Option[Rep[RDDIndexedCollection[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[RDDIndexedCollection[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object slice {
      def unapply(d: Def[_]): Option[(Rep[RDDIndexedCollection[A]], Rep[Int], Rep[Int]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(offset, length, _*), _) if receiver.elem.isInstanceOf[RDDIndexedCollectionElem[_]] && method.getName == "slice" =>
          Some((receiver, offset, length)).asInstanceOf[Option[(Rep[RDDIndexedCollection[A]], Rep[Int], Rep[Int]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDIndexedCollection[A]], Rep[Int], Rep[Int]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object apply_many {
      def unapply(d: Def[_]): Option[(Rep[RDDIndexedCollection[A]], Coll[Int]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(indices, _*), _) if receiver.elem.isInstanceOf[RDDIndexedCollectionElem[_]] && method.getName == "apply" && { val ann = method.getAnnotation(classOf[scalan.OverloadId]); ann != null && ann.value == "many" } =>
          Some((receiver, indices)).asInstanceOf[Option[(Rep[RDDIndexedCollection[A]], Coll[Int]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDIndexedCollection[A]], Coll[Int]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object mapBy {
      def unapply(d: Def[_]): Option[(Rep[RDDIndexedCollection[A]], Rep[A @uncheckedVariance => B]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[RDDIndexedCollectionElem[_]] && method.getName == "mapBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[RDDIndexedCollection[A]], Rep[A @uncheckedVariance => B]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDIndexedCollection[A]], Rep[A @uncheckedVariance => B]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object reduce {
      def unapply(d: Def[_]): Option[(Rep[RDDIndexedCollection[A]], RepMonoid[A @uncheckedVariance]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(m, _*), _) if receiver.elem.isInstanceOf[RDDIndexedCollectionElem[_]] && method.getName == "reduce" =>
          Some((receiver, m)).asInstanceOf[Option[(Rep[RDDIndexedCollection[A]], RepMonoid[A @uncheckedVariance]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDIndexedCollection[A]], RepMonoid[A @uncheckedVariance]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object zip {
      def unapply(d: Def[_]): Option[(Rep[RDDIndexedCollection[A]], Coll[B]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(ys, _*), _) if receiver.elem.isInstanceOf[RDDIndexedCollectionElem[_]] && method.getName == "zip" =>
          Some((receiver, ys)).asInstanceOf[Option[(Rep[RDDIndexedCollection[A]], Coll[B]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDIndexedCollection[A]], Coll[B]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object update {
      def unapply(d: Def[_]): Option[(Rep[RDDIndexedCollection[A]], Rep[Int], Rep[A]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(idx, value, _*), _) if receiver.elem.isInstanceOf[RDDIndexedCollectionElem[_]] && method.getName == "update" =>
          Some((receiver, idx, value)).asInstanceOf[Option[(Rep[RDDIndexedCollection[A]], Rep[Int], Rep[A]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDIndexedCollection[A]], Rep[Int], Rep[A]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object updateMany {
      def unapply(d: Def[_]): Option[(Rep[RDDIndexedCollection[A]], Coll[Int], Coll[A]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(idxs, vals, _*), _) if receiver.elem.isInstanceOf[RDDIndexedCollectionElem[_]] && method.getName == "updateMany" =>
          Some((receiver, idxs, vals)).asInstanceOf[Option[(Rep[RDDIndexedCollection[A]], Coll[Int], Coll[A]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDIndexedCollection[A]], Coll[Int], Coll[A]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object filterBy {
      def unapply(d: Def[_]): Option[(Rep[RDDIndexedCollection[A]], Rep[A @uncheckedVariance => Boolean]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[RDDIndexedCollectionElem[_]] && method.getName == "filterBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[RDDIndexedCollection[A]], Rep[A @uncheckedVariance => Boolean]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDIndexedCollection[A]], Rep[A @uncheckedVariance => Boolean]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object flatMapBy {
      def unapply(d: Def[_]): Option[(Rep[RDDIndexedCollection[A]], Rep[A @uncheckedVariance => Collection[B]]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[RDDIndexedCollectionElem[_]] && method.getName == "flatMapBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[RDDIndexedCollection[A]], Rep[A @uncheckedVariance => Collection[B]]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDIndexedCollection[A]], Rep[A @uncheckedVariance => Collection[B]]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object append {
      def unapply(d: Def[_]): Option[(Rep[RDDIndexedCollection[A]], Rep[A @uncheckedVariance]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(value, _*), _) if receiver.elem.isInstanceOf[RDDIndexedCollectionElem[_]] && method.getName == "append" =>
          Some((receiver, value)).asInstanceOf[Option[(Rep[RDDIndexedCollection[A]], Rep[A @uncheckedVariance]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDIndexedCollection[A]], Rep[A @uncheckedVariance]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object RDDIndexedCollectionCompanionMethods {
  }

  def mkRDDIndexedCollection[A]
    (indexedRdd: RepRDD[(Long, A)])(implicit eItem: Elem[A]): Rep[RDDIndexedCollection[A]] =
    new ExpRDDIndexedCollection[A](indexedRdd)
  def unmkRDDIndexedCollection[A](p: Rep[IRDDCollection[A]]) = p.elem.asInstanceOf[Elem[_]] match {
    case _: RDDIndexedCollectionElem[A] @unchecked =>
      Some((p.asRep[RDDIndexedCollection[A]].indexedRdd))
    case _ =>
      None
  }

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
        case MethodCall(receiver, method, Seq(i, _*), _) if receiver.elem.isInstanceOf[PairRDDCollectionElem[_, _]] && method.getName == "apply" && method.getAnnotation(classOf[scalan.OverloadId]) == null =>
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
      def unapply(d: Def[_]): Option[(Rep[PairRDDCollection[A, B]], Rep[(A, B) @uncheckedVariance => C]) forSome {type A; type B; type C}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[PairRDDCollectionElem[_, _]] && method.getName == "mapBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[PairRDDCollection[A, B]], Rep[(A, B) @uncheckedVariance => C]) forSome {type A; type B; type C}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDCollection[A, B]], Rep[(A, B) @uncheckedVariance => C]) forSome {type A; type B; type C}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object reduce {
      def unapply(d: Def[_]): Option[(Rep[PairRDDCollection[A, B]], RepMonoid[(A, B) @uncheckedVariance]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(m, _*), _) if receiver.elem.isInstanceOf[PairRDDCollectionElem[_, _]] && method.getName == "reduce" =>
          Some((receiver, m)).asInstanceOf[Option[(Rep[PairRDDCollection[A, B]], RepMonoid[(A, B) @uncheckedVariance]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDCollection[A, B]], RepMonoid[(A, B) @uncheckedVariance]) forSome {type A; type B}] = exp match {
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
      def unapply(d: Def[_]): Option[(Rep[PairRDDCollection[A, B]], Rep[(A, B) @uncheckedVariance => Boolean]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[PairRDDCollectionElem[_, _]] && method.getName == "filterBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[PairRDDCollection[A, B]], Rep[(A, B) @uncheckedVariance => Boolean]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDCollection[A, B]], Rep[(A, B) @uncheckedVariance => Boolean]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object flatMapBy {
      def unapply(d: Def[_]): Option[(Rep[PairRDDCollection[A, B]], Rep[(A, B) @uncheckedVariance => Collection[C]]) forSome {type A; type B; type C}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[PairRDDCollectionElem[_, _]] && method.getName == "flatMapBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[PairRDDCollection[A, B]], Rep[(A, B) @uncheckedVariance => Collection[C]]) forSome {type A; type B; type C}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDCollection[A, B]], Rep[(A, B) @uncheckedVariance => Collection[C]]) forSome {type A; type B; type C}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object append {
      def unapply(d: Def[_]): Option[(Rep[PairRDDCollection[A, B]], Rep[(A, B) @uncheckedVariance]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(value, _*), _) if receiver.elem.isInstanceOf[PairRDDCollectionElem[_, _]] && method.getName == "append" =>
          Some((receiver, value)).asInstanceOf[Option[(Rep[PairRDDCollection[A, B]], Rep[(A, B) @uncheckedVariance]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDCollection[A, B]], Rep[(A, B) @uncheckedVariance]) forSome {type A; type B}] = exp match {
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
  def unmkPairRDDCollection[A, B](p: Rep[IRDDPairCollection[A, B]]) = p.elem.asInstanceOf[Elem[_]] match {
    case _: PairRDDCollectionElem[A, B] @unchecked =>
      Some((p.asRep[PairRDDCollection[A, B]].pairRDD))
    case _ =>
      None
  }

  case class ExpPairRDDIndexedCollection[A, B]
      (override val indices: RepRDD[Long], override val pairRDD: RepRDD[(A, B)])
      (implicit eA: Elem[A], eB: Elem[B])
    extends PairRDDIndexedCollection[A, B](indices, pairRDD) with UserTypeDef[PairRDDIndexedCollection[A, B]] {
    lazy val selfType = element[PairRDDIndexedCollection[A, B]]
    override def mirror(t: Transformer) = ExpPairRDDIndexedCollection[A, B](t(indices), t(pairRDD))
  }

  lazy val PairRDDIndexedCollection: Rep[PairRDDIndexedCollectionCompanionAbs] = new PairRDDIndexedCollectionCompanionAbs with UserTypeDef[PairRDDIndexedCollectionCompanionAbs] {
    lazy val selfType = element[PairRDDIndexedCollectionCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object PairRDDIndexedCollectionMethods {
    object as {
      def unapply(d: Def[_]): Option[Rep[PairRDDIndexedCollection[A, B]] forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[PairRDDIndexedCollectionElem[_, _]] && method.getName == "as" =>
          Some(receiver).asInstanceOf[Option[Rep[PairRDDIndexedCollection[A, B]] forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[PairRDDIndexedCollection[A, B]] forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object bs {
      def unapply(d: Def[_]): Option[Rep[PairRDDIndexedCollection[A, B]] forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[PairRDDIndexedCollectionElem[_, _]] && method.getName == "bs" =>
          Some(receiver).asInstanceOf[Option[Rep[PairRDDIndexedCollection[A, B]] forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[PairRDDIndexedCollection[A, B]] forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object arr {
      def unapply(d: Def[_]): Option[Rep[PairRDDIndexedCollection[A, B]] forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[PairRDDIndexedCollectionElem[_, _]] && method.getName == "arr" =>
          Some(receiver).asInstanceOf[Option[Rep[PairRDDIndexedCollection[A, B]] forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[PairRDDIndexedCollection[A, B]] forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object lst {
      def unapply(d: Def[_]): Option[Rep[PairRDDIndexedCollection[A, B]] forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[PairRDDIndexedCollectionElem[_, _]] && method.getName == "lst" =>
          Some(receiver).asInstanceOf[Option[Rep[PairRDDIndexedCollection[A, B]] forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[PairRDDIndexedCollection[A, B]] forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object apply {
      def unapply(d: Def[_]): Option[(Rep[PairRDDIndexedCollection[A, B]], Rep[Int]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(i, _*), _) if receiver.elem.isInstanceOf[PairRDDIndexedCollectionElem[_, _]] && method.getName == "apply" && method.getAnnotation(classOf[scalan.OverloadId]) == null =>
          Some((receiver, i)).asInstanceOf[Option[(Rep[PairRDDIndexedCollection[A, B]], Rep[Int]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDIndexedCollection[A, B]], Rep[Int]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object length {
      def unapply(d: Def[_]): Option[Rep[PairRDDIndexedCollection[A, B]] forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[PairRDDIndexedCollectionElem[_, _]] && method.getName == "length" =>
          Some(receiver).asInstanceOf[Option[Rep[PairRDDIndexedCollection[A, B]] forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[PairRDDIndexedCollection[A, B]] forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object slice {
      def unapply(d: Def[_]): Option[(Rep[PairRDDIndexedCollection[A, B]], Rep[Int], Rep[Int]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(offset, length, _*), _) if receiver.elem.isInstanceOf[PairRDDIndexedCollectionElem[_, _]] && method.getName == "slice" =>
          Some((receiver, offset, length)).asInstanceOf[Option[(Rep[PairRDDIndexedCollection[A, B]], Rep[Int], Rep[Int]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDIndexedCollection[A, B]], Rep[Int], Rep[Int]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object apply_many {
      def unapply(d: Def[_]): Option[(Rep[PairRDDIndexedCollection[A, B]], Coll[Int]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(idxs, _*), _) if receiver.elem.isInstanceOf[PairRDDIndexedCollectionElem[_, _]] && method.getName == "apply" && { val ann = method.getAnnotation(classOf[scalan.OverloadId]); ann != null && ann.value == "many" } =>
          Some((receiver, idxs)).asInstanceOf[Option[(Rep[PairRDDIndexedCollection[A, B]], Coll[Int]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDIndexedCollection[A, B]], Coll[Int]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object mapBy {
      def unapply(d: Def[_]): Option[(Rep[PairRDDIndexedCollection[A, B]], Rep[(A, B) @uncheckedVariance => C]) forSome {type A; type B; type C}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[PairRDDIndexedCollectionElem[_, _]] && method.getName == "mapBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[PairRDDIndexedCollection[A, B]], Rep[(A, B) @uncheckedVariance => C]) forSome {type A; type B; type C}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDIndexedCollection[A, B]], Rep[(A, B) @uncheckedVariance => C]) forSome {type A; type B; type C}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object reduce {
      def unapply(d: Def[_]): Option[(Rep[PairRDDIndexedCollection[A, B]], RepMonoid[(A, B) @uncheckedVariance]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(m, _*), _) if receiver.elem.isInstanceOf[PairRDDIndexedCollectionElem[_, _]] && method.getName == "reduce" =>
          Some((receiver, m)).asInstanceOf[Option[(Rep[PairRDDIndexedCollection[A, B]], RepMonoid[(A, B) @uncheckedVariance]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDIndexedCollection[A, B]], RepMonoid[(A, B) @uncheckedVariance]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object zip {
      def unapply(d: Def[_]): Option[(Rep[PairRDDIndexedCollection[A, B]], Coll[C]) forSome {type A; type B; type C}] = d match {
        case MethodCall(receiver, method, Seq(ys, _*), _) if receiver.elem.isInstanceOf[PairRDDIndexedCollectionElem[_, _]] && method.getName == "zip" =>
          Some((receiver, ys)).asInstanceOf[Option[(Rep[PairRDDIndexedCollection[A, B]], Coll[C]) forSome {type A; type B; type C}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDIndexedCollection[A, B]], Coll[C]) forSome {type A; type B; type C}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object update {
      def unapply(d: Def[_]): Option[(Rep[PairRDDIndexedCollection[A, B]], Rep[Int], Rep[(A, B)]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(idx, value, _*), _) if receiver.elem.isInstanceOf[PairRDDIndexedCollectionElem[_, _]] && method.getName == "update" =>
          Some((receiver, idx, value)).asInstanceOf[Option[(Rep[PairRDDIndexedCollection[A, B]], Rep[Int], Rep[(A, B)]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDIndexedCollection[A, B]], Rep[Int], Rep[(A, B)]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object updateMany {
      def unapply(d: Def[_]): Option[(Rep[PairRDDIndexedCollection[A, B]], Coll[Int], Coll[(A, B)]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(idxs, vals, _*), _) if receiver.elem.isInstanceOf[PairRDDIndexedCollectionElem[_, _]] && method.getName == "updateMany" =>
          Some((receiver, idxs, vals)).asInstanceOf[Option[(Rep[PairRDDIndexedCollection[A, B]], Coll[Int], Coll[(A, B)]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDIndexedCollection[A, B]], Coll[Int], Coll[(A, B)]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object filterBy {
      def unapply(d: Def[_]): Option[(Rep[PairRDDIndexedCollection[A, B]], Rep[(A, B) @uncheckedVariance => Boolean]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[PairRDDIndexedCollectionElem[_, _]] && method.getName == "filterBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[PairRDDIndexedCollection[A, B]], Rep[(A, B) @uncheckedVariance => Boolean]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDIndexedCollection[A, B]], Rep[(A, B) @uncheckedVariance => Boolean]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object flatMapBy {
      def unapply(d: Def[_]): Option[(Rep[PairRDDIndexedCollection[A, B]], Rep[(A, B) @uncheckedVariance => Collection[C]]) forSome {type A; type B; type C}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[PairRDDIndexedCollectionElem[_, _]] && method.getName == "flatMapBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[PairRDDIndexedCollection[A, B]], Rep[(A, B) @uncheckedVariance => Collection[C]]) forSome {type A; type B; type C}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDIndexedCollection[A, B]], Rep[(A, B) @uncheckedVariance => Collection[C]]) forSome {type A; type B; type C}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object append {
      def unapply(d: Def[_]): Option[(Rep[PairRDDIndexedCollection[A, B]], Rep[(A, B) @uncheckedVariance]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(value, _*), _) if receiver.elem.isInstanceOf[PairRDDIndexedCollectionElem[_, _]] && method.getName == "append" =>
          Some((receiver, value)).asInstanceOf[Option[(Rep[PairRDDIndexedCollection[A, B]], Rep[(A, B) @uncheckedVariance]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairRDDIndexedCollection[A, B]], Rep[(A, B) @uncheckedVariance]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object PairRDDIndexedCollectionCompanionMethods {
  }

  def mkPairRDDIndexedCollection[A, B]
    (indices: RepRDD[Long], pairRDD: RepRDD[(A, B)])(implicit eA: Elem[A], eB: Elem[B]): Rep[PairRDDIndexedCollection[A, B]] =
    new ExpPairRDDIndexedCollection[A, B](indices, pairRDD)
  def unmkPairRDDIndexedCollection[A, B](p: Rep[IRDDPairCollection[A, B]]) = p.elem.asInstanceOf[Elem[_]] match {
    case _: PairRDDIndexedCollectionElem[A, B] @unchecked =>
      Some((p.asRep[PairRDDIndexedCollection[A, B]].indices, p.asRep[PairRDDIndexedCollection[A, B]].pairRDD))
    case _ =>
      None
  }

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
        case MethodCall(receiver, method, Seq(i, _*), _) if receiver.elem.isInstanceOf[RDDNestedCollectionElem[_]] && method.getName == "apply" && method.getAnnotation(classOf[scalan.OverloadId]) == null =>
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
      def unapply(d: Def[_]): Option[(Rep[RDDNestedCollection[A]], Rep[Collection[A @uncheckedVariance] => B @uncheckedVariance]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[RDDNestedCollectionElem[_]] && method.getName == "mapBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[RDDNestedCollection[A]], Rep[Collection[A @uncheckedVariance] => B @uncheckedVariance]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDNestedCollection[A]], Rep[Collection[A @uncheckedVariance] => B @uncheckedVariance]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object reduce {
      def unapply(d: Def[_]): Option[(Rep[RDDNestedCollection[A]], RepMonoid[Collection[A @uncheckedVariance]]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(m, _*), _) if receiver.elem.isInstanceOf[RDDNestedCollectionElem[_]] && method.getName == "reduce" =>
          Some((receiver, m)).asInstanceOf[Option[(Rep[RDDNestedCollection[A]], RepMonoid[Collection[A @uncheckedVariance]]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDNestedCollection[A]], RepMonoid[Collection[A @uncheckedVariance]]) forSome {type A}] = exp match {
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
      def unapply(d: Def[_]): Option[(Rep[RDDNestedCollection[A]], Rep[Collection[A @uncheckedVariance] => Boolean]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[RDDNestedCollectionElem[_]] && method.getName == "filterBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[RDDNestedCollection[A]], Rep[Collection[A @uncheckedVariance] => Boolean]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDNestedCollection[A]], Rep[Collection[A @uncheckedVariance] => Boolean]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object flatMapBy {
      def unapply(d: Def[_]): Option[(Rep[RDDNestedCollection[A]], Rep[Collection[A @uncheckedVariance] => Collection[B]]) forSome {type A; type B}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[RDDNestedCollectionElem[_]] && method.getName == "flatMapBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[RDDNestedCollection[A]], Rep[Collection[A @uncheckedVariance] => Collection[B]]) forSome {type A; type B}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDNestedCollection[A]], Rep[Collection[A @uncheckedVariance] => Collection[B]]) forSome {type A; type B}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object append {
      def unapply(d: Def[_]): Option[(Rep[RDDNestedCollection[A]], Rep[Collection[A @uncheckedVariance]]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(value, _*), _) if receiver.elem.isInstanceOf[RDDNestedCollectionElem[_]] && method.getName == "append" =>
          Some((receiver, value)).asInstanceOf[Option[(Rep[RDDNestedCollection[A]], Rep[Collection[A @uncheckedVariance]]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[RDDNestedCollection[A]], Rep[Collection[A @uncheckedVariance]]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object RDDNestedCollectionCompanionMethods {
    object createRDDNestedCollection {
      def unapply(d: Def[_]): Option[(Rep[RDDCollection[A]], Rep[PairRDDCollection[Int,Int]]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(vals, segments, _*), _) if receiver.elem == RDDNestedCollectionCompanionElem && method.getName == "createRDDNestedCollection" =>
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
  def unmkRDDNestedCollection[A](p: Rep[IRDDNestedCollection[A]]) = p.elem.asInstanceOf[Elem[_]] match {
    case _: RDDNestedCollectionElem[A] @unchecked =>
      Some((p.asRep[RDDNestedCollection[A]].values, p.asRep[RDDNestedCollection[A]].segments))
    case _ =>
      None
  }

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
