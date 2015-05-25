package scalan.spark
package impl

import org.apache.spark.rdd._
import scalan._
import scalan.common.Default
import org.apache.spark.Partitioner
import scala.reflect._
import scala.reflect.runtime.universe._
import scalan.common.Default

// Abs -----------------------------------
trait PairRDDFunctionssAbs extends PairRDDFunctionss with ScalanCommunityDsl {
  self: SparkDsl =>

  // single proxy for each type family
  implicit def proxySPairRDDFunctions[K, V](p: Rep[SPairRDDFunctions[K, V]]): SPairRDDFunctions[K, V] = {
    proxyOps[SPairRDDFunctions[K, V]](p)(classTag[SPairRDDFunctions[K, V]])
  }

  // TypeWrapper proxy
  //implicit def proxyPairRDDFunctions[K:Elem, V:Elem](p: Rep[PairRDDFunctions[K, V]]): SPairRDDFunctions[K, V] =
  //  proxyOps[SPairRDDFunctions[K, V]](p.asRep[SPairRDDFunctions[K, V]])

  implicit def unwrapValueOfSPairRDDFunctions[K, V](w: Rep[SPairRDDFunctions[K, V]]): Rep[PairRDDFunctions[K, V]] = w.wrappedValueOfBaseType

  implicit def pairRDDFunctionsElement[K:Elem, V:Elem]: Elem[PairRDDFunctions[K, V]]

  case class SPairRDDFunctionsIso[A1, A2, B1, B2](iso: PairIso[A1, A2, B1, B2]) //(implicit val eA1: Elem[A1], implicit val eA2: Elem[A2], implicit val eB1: Elem[B1], implicit val eB2: Elem[B2])
    extends Iso[SPairRDDFunctions[A1, A2], SPairRDDFunctions[B1, B2]]()(sPairRDDFunctionsElement(iso.eA1, iso.eA2)) {
    implicit val eA = iso.eFrom
    implicit val eB = iso.eTo
    implicit val eA1 = iso.eA1
    implicit val eA2 = iso.eA2
    implicit val eB1 = iso.eB1
    implicit val eB2 = iso.eB2
    lazy val eTo = sPairRDDFunctionsElement(eB1, eB2)
    lazy val tag = weakTypeTag[SPairRDDFunctions[B1, B2]]
    override def isIdentity = iso.isIdentity

    def from(x: Rep[SPairRDDFunctions[B1, B2]]) = SPairRDDFunctions((x.keys zip x.values).map(iso.from _))
    def to(x: Rep[SPairRDDFunctions[A1, A2]]) = SPairRDDFunctions((x.keys zip x.values).map(iso.to _))
    lazy val defaultRepTo = Default.defaultVal(SPairRDDFunctions(SRDD.empty(eB)))
  }

  // familyElem
  abstract class SPairRDDFunctionsElem[K, V, To <: SPairRDDFunctions[K, V]](implicit val eK: Elem[K], val eV: Elem[V])
    extends WrapperElem[PairRDDFunctions[K,V], To] {
    override def isEntityType = true
    override def tag = {
      implicit val tagK = eK.tag
      implicit val tagV = eV.tag
      weakTypeTag[SPairRDDFunctions[K, V]].asInstanceOf[WeakTypeTag[To]]
    }
    override def convert(x: Rep[Reifiable[_]]) = convertSPairRDDFunctions(x.asRep[SPairRDDFunctions[K, V]])
    def convertSPairRDDFunctions(x : Rep[SPairRDDFunctions[K, V]]): Rep[To] = {
      //assert(x.selfType1.isInstanceOf[SPairRDDFunctionsElem[_,_,_]])
      x.asRep[To]
    }
    override def getDefaultRep: Rep[To] = ???
  }

  implicit def sPairRDDFunctionsElement[K, V](implicit eK: Elem[K], eV: Elem[V]): Elem[SPairRDDFunctions[K, V]] =
    new SPairRDDFunctionsElem[K, V, SPairRDDFunctions[K, V]] {
      lazy val eTo = element[SPairRDDFunctionsImpl[K, V]]
    }

  trait SPairRDDFunctionsCompanionElem extends CompanionElem[SPairRDDFunctionsCompanionAbs]
  implicit lazy val SPairRDDFunctionsCompanionElem: SPairRDDFunctionsCompanionElem = new SPairRDDFunctionsCompanionElem {
    lazy val tag = weakTypeTag[SPairRDDFunctionsCompanionAbs]
    protected def getDefaultRep = SPairRDDFunctions
  }

  abstract class SPairRDDFunctionsCompanionAbs extends CompanionBase[SPairRDDFunctionsCompanionAbs] with SPairRDDFunctionsCompanion {
    override def toString = "SPairRDDFunctions"

    def apply[K:Elem, V:Elem](rdd: Rep[SRDD[(K, V)]]): Rep[SPairRDDFunctions[K,V]] =
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

    def partitionBy(partitioner: Rep[SPartitioner]): Rep[SRDD[(K, V)]] =
      methodCallEx[SRDD[(K, V)]](self,
        this.getClass.getMethod("partitionBy", classOf[AnyRef]),
        List(partitioner.asInstanceOf[AnyRef]))

    def reduceByKey(func: Rep[((V, V)) => V]): Rep[SRDD[(K, V)]] =
      methodCallEx[SRDD[(K, V)]](self,
        this.getClass.getMethod("reduceByKey", classOf[AnyRef]),
        List(func.asInstanceOf[AnyRef]))

    def lookup(key: Rep[K]): Rep[SSeq[V]] =
      methodCallEx[SSeq[V]](self,
        this.getClass.getMethod("lookup", classOf[AnyRef]),
        List(key.asInstanceOf[AnyRef]))

    def groupByKey: Rep[SRDD[(K, SSeq[V])]] =
      methodCallEx[SRDD[(K, SSeq[V])]](self,
        this.getClass.getMethod("groupByKey"),
        List())

    def countByKey: Rep[MMap[K,Long]] =
      methodCallEx[MMap[K,Long]](self,
        this.getClass.getMethod("countByKey"),
        List())

    def foldByKey(zeroValue: Rep[V])(op: Rep[((V, V)) => V]): Rep[SRDD[(K, V)]] =
      methodCallEx[SRDD[(K, V)]](self,
        this.getClass.getMethod("foldByKey", classOf[AnyRef], classOf[AnyRef]),
        List(zeroValue.asInstanceOf[AnyRef], op.asInstanceOf[AnyRef]))

    def join[W:Elem](other: Rep[SRDD[(K, W)]]): Rep[SRDD[(K, (V, W))]] = {
      methodCallEx[SRDD[(K, (V, W))]](self,
        this.getClass.getMethod("join", classOf[AnyRef], classOf[Elem[W]]),
        List(other.asInstanceOf[AnyRef], element[W])) (sRDDElement(PairElem(element[K], PairElem(element[V], element[W]))))
    }
    def groupWithExt[W:Elem](other: Rep[SRDD[(K, W)]]): Rep[SRDD[(K, (SSeq[V], SSeq[W]))]] =
      methodCallEx[SRDD[(K, (SSeq[V], SSeq[W]))]](self,
        this.getClass.getMethod("groupWithExt", classOf[AnyRef], classOf[Elem[W]]),
        List(other.asInstanceOf[AnyRef], element[W]))
  }
  trait SPairRDDFunctionsImplCompanion
  // elem for concrete class
  class SPairRDDFunctionsImplElem[K, V](val iso: Iso[SPairRDDFunctionsImplData[K, V], SPairRDDFunctionsImpl[K, V]])(implicit eK: Elem[K], eV: Elem[V])
    extends SPairRDDFunctionsElem[K, V, SPairRDDFunctionsImpl[K, V]]
    with ConcreteElem[SPairRDDFunctionsImplData[K, V], SPairRDDFunctionsImpl[K, V]] {
    lazy val eTo = this
    override def convertSPairRDDFunctions(x: Rep[SPairRDDFunctions[K, V]]) = SPairRDDFunctionsImpl(x.wrappedValueOfBaseType)
    override def getDefaultRep = super[ConcreteElem].getDefaultRep
    override lazy val tag = super[ConcreteElem].tag
  }

  // state representation type
  type SPairRDDFunctionsImplData[K, V] = PairRDDFunctions[K,V]

  // 3) Iso for concrete class
  class SPairRDDFunctionsImplIso[K, V](implicit eK: Elem[K], eV: Elem[V])
    extends Iso[SPairRDDFunctionsImplData[K, V], SPairRDDFunctionsImpl[K, V]] {
    override def from(p: Rep[SPairRDDFunctionsImpl[K, V]]) =
      p.wrappedValueOfBaseType
    override def to(p: Rep[PairRDDFunctions[K,V]]) = {
      val wrappedValueOfBaseType = p
      SPairRDDFunctionsImpl(wrappedValueOfBaseType)
    }
    lazy val tag = {
      implicit val tagK = eK.tag
      implicit val tagV = eV.tag
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
  }
  object SPairRDDFunctionsImplMatcher {
    def unapply[K, V](p: Rep[SPairRDDFunctions[K, V]]) = unmkSPairRDDFunctionsImpl(p)
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
  def unmkSPairRDDFunctionsImpl[K, V](p: Rep[SPairRDDFunctions[K, V]]): Option[(Rep[PairRDDFunctions[K,V]])]
}

// Seq -----------------------------------
trait PairRDDFunctionssSeq extends PairRDDFunctionssDsl with ScalanCommunityDslSeq {
  self: SparkDslSeq =>
  lazy val SPairRDDFunctions: Rep[SPairRDDFunctionsCompanionAbs] = new SPairRDDFunctionsCompanionAbs with UserTypeSeq[SPairRDDFunctionsCompanionAbs] {
    lazy val selfType = element[SPairRDDFunctionsCompanionAbs]

    override def apply[K:Elem, V:Elem](rdd: Rep[SRDD[(K, V)]]): Rep[SPairRDDFunctions[K,V]] =
      SPairRDDFunctionsImpl(new PairRDDFunctions[K, V](rdd))
  }

    // override proxy if we deal with TypeWrapper
  //override def proxyPairRDDFunctions[K:Elem, V:Elem](p: Rep[PairRDDFunctions[K, V]]): SPairRDDFunctions[K, V] =
  //  proxyOpsEx[PairRDDFunctions[K, V],SPairRDDFunctions[K, V], SeqSPairRDDFunctionsImpl[K, V]](p, bt => SeqSPairRDDFunctionsImpl(bt))

    implicit def pairRDDFunctionsElement[K:Elem, V:Elem]: Elem[PairRDDFunctions[K, V]] = new SeqBaseElemEx[PairRDDFunctions[K, V], SPairRDDFunctions[K, V]](element[SPairRDDFunctions[K, V]])(weakTypeTag[PairRDDFunctions[K, V]], DefaultOfPairRDDFunctions[K, V])

  case class SeqSPairRDDFunctionsImpl[K, V]
      (override val wrappedValueOfBaseType: Rep[PairRDDFunctions[K,V]])
      (implicit eK: Elem[K], eV: Elem[V])
    extends SPairRDDFunctionsImpl[K, V](wrappedValueOfBaseType)
        with UserTypeSeq[SPairRDDFunctionsImpl[K, V]] {
    lazy val selfType = element[SPairRDDFunctionsImpl[K, V]]
    override def keys: Rep[SRDD[K]] =
      wrappedValueOfBaseType.keys

    override def values: Rep[SRDD[V]] =
      wrappedValueOfBaseType.values

    override def partitionBy(partitioner: Rep[SPartitioner]): Rep[SRDD[(K, V)]] =
      wrappedValueOfBaseType.partitionBy(partitioner)

    override def reduceByKey(func: Rep[((V, V)) => V]): Rep[SRDD[(K, V)]] =
      wrappedValueOfBaseType.reduceByKey(scala.Function.untupled(func))

    override def lookup(key: Rep[K]): Rep[SSeq[V]] =
      wrappedValueOfBaseType.lookup(key)

    override def groupByKey: Rep[SRDD[(K, SSeq[V])]] =
      ??? //wrappedValueOfBaseType.groupByKey

    override def countByKey: Rep[MMap[K,Long]] =
      ??? //wrappedValueOfBaseType.countByKey

    override def foldByKey(zeroValue: Rep[V])(op: Rep[((V, V)) => V]): Rep[SRDD[(K, V)]] =
      ??? //wrappedValueOfBaseType.foldByKey(zeroValue)(scala.Function.untupled(op))

    override def join[W:Elem](other: Rep[SRDD[(K, W)]]): Rep[SRDD[(K, (V, W))]] =
      ??? //wrappedValueOfBaseType.join[W](other)

    override def groupWithExt[W:Elem](other: Rep[SRDD[(K, W)]]): Rep[SRDD[(K, (SSeq[V], SSeq[W]))]] =
      wrappedValueOfBaseType.groupWithExt[W](other)
  }
  lazy val SPairRDDFunctionsImpl = new SPairRDDFunctionsImplCompanionAbs with UserTypeSeq[SPairRDDFunctionsImplCompanionAbs] {
    lazy val selfType = element[SPairRDDFunctionsImplCompanionAbs]
  }

  def mkSPairRDDFunctionsImpl[K, V]
      (wrappedValueOfBaseType: Rep[PairRDDFunctions[K,V]])(implicit eK: Elem[K], eV: Elem[V]): Rep[SPairRDDFunctionsImpl[K, V]] =
      new SeqSPairRDDFunctionsImpl[K, V](wrappedValueOfBaseType)
  def unmkSPairRDDFunctionsImpl[K, V](p: Rep[SPairRDDFunctions[K, V]]) = p match {
    case p: SPairRDDFunctionsImpl[K, V] @unchecked =>
      Some((p.wrappedValueOfBaseType))
    case _ => None
  }

  implicit def wrapPairRDDFunctionsToSPairRDDFunctions[K:Elem, V:Elem](v: PairRDDFunctions[K, V]): SPairRDDFunctions[K, V] = SPairRDDFunctionsImpl(v)
}

// Exp -----------------------------------
trait PairRDDFunctionssExp extends PairRDDFunctionssDsl with ScalanCommunityDslExp {
  self: SparkDslExp =>
  lazy val SPairRDDFunctions: Rep[SPairRDDFunctionsCompanionAbs] = new SPairRDDFunctionsCompanionAbs with UserTypeDef[SPairRDDFunctionsCompanionAbs] {
    lazy val selfType = element[SPairRDDFunctionsCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  case class ViewSPairRDDFunctions[A1, A2, B1, B2](source: Rep[SPairRDDFunctions[A1, A2]])(implicit val innerIso: PairIso[A1,A2, B1,B2])
    extends View[SPairRDDFunctions[A1, A2], SPairRDDFunctions[B1, B2]] {
    lazy val iso = SPairRDDFunctionsIso(innerIso)
    def copy(source: Rep[SPairRDDFunctions[A1,A2]]) = ViewSPairRDDFunctions(source)(innerIso)
    override def toString = s"ViewSPairRDDFunctions[${iso.eTo.name}]($source)"
    override def equals(other: Any) = other match {
      case v: ViewSPairRDDFunctions[_, _, _, _] => source == v.source && iso.eTo == v.iso.eTo
      case _ => false
    }
  }

  implicit def pairRDDFunctionsElement[K:Elem, V:Elem]: Elem[PairRDDFunctions[K, V]] = new ExpBaseElemEx[PairRDDFunctions[K, V], SPairRDDFunctions[K, V]](element[SPairRDDFunctions[K, V]])(weakTypeTag[PairRDDFunctions[K, V]], DefaultOfPairRDDFunctions[K, V])

  case class ExpSPairRDDFunctionsImpl[K, V]
      (override val wrappedValueOfBaseType: Rep[PairRDDFunctions[K,V]])
      (implicit eK: Elem[K], eV: Elem[V])
    extends SPairRDDFunctionsImpl[K, V](wrappedValueOfBaseType) with UserTypeDef[SPairRDDFunctionsImpl[K, V]] {
    lazy val selfType = element[SPairRDDFunctionsImpl[K, V]]
    override def mirror(t: Transformer) = ExpSPairRDDFunctionsImpl[K, V](t(wrappedValueOfBaseType))
  }

  lazy val SPairRDDFunctionsImpl: Rep[SPairRDDFunctionsImplCompanionAbs] = new SPairRDDFunctionsImplCompanionAbs with UserTypeDef[SPairRDDFunctionsImplCompanionAbs] {
    lazy val selfType = element[SPairRDDFunctionsImplCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object SPairRDDFunctionsImplMethods {
  }

  def mkSPairRDDFunctionsImpl[K, V]
    (wrappedValueOfBaseType: Rep[PairRDDFunctions[K,V]])(implicit eK: Elem[K], eV: Elem[V]): Rep[SPairRDDFunctionsImpl[K, V]] =
    new ExpSPairRDDFunctionsImpl[K, V](wrappedValueOfBaseType)
  def unmkSPairRDDFunctionsImpl[K, V](p: Rep[SPairRDDFunctions[K, V]]) = p.elem.asInstanceOf[Elem[_]] match {
    case _: SPairRDDFunctionsImplElem[K, V] @unchecked =>
      Some((p.asRep[SPairRDDFunctionsImpl[K, V]].wrappedValueOfBaseType))
    case _ =>
      None
  }

  object SPairRDDFunctionsMethods {
    object wrappedValueOfBaseType {
      def unapply(d: Def[_]): Option[Rep[SPairRDDFunctions[K, V]] forSome {type K; type V}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SPairRDDFunctionsElem[_, _, _]] && method.getName == "wrappedValueOfBaseType" =>
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
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SPairRDDFunctionsElem[_, _, _]] && method.getName == "keys" =>
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
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SPairRDDFunctionsElem[_, _, _]] && method.getName == "values" =>
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
        case MethodCall(receiver, method, Seq(partitioner, _*), _) if receiver.elem.isInstanceOf[SPairRDDFunctionsElem[_, _, _]] && method.getName == "partitionBy" =>
          Some((receiver, partitioner)).asInstanceOf[Option[(Rep[SPairRDDFunctions[K, V]], Rep[SPartitioner]) forSome {type K; type V}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SPairRDDFunctions[K, V]], Rep[SPartitioner]) forSome {type K; type V}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object reduceByKey {
      def unapply(d: Def[_]): Option[(Rep[SPairRDDFunctions[K, V]], Rep[((V, V)) => V]) forSome {type K; type V}] = d match {
        case MethodCall(receiver, method, Seq(func, _*), _) if receiver.elem.isInstanceOf[SPairRDDFunctionsElem[_, _, _]] && method.getName == "reduceByKey" =>
          Some((receiver, func)).asInstanceOf[Option[(Rep[SPairRDDFunctions[K, V]], Rep[((V, V)) => V]) forSome {type K; type V}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SPairRDDFunctions[K, V]], Rep[((V, V)) => V]) forSome {type K; type V}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object lookup {
      def unapply(d: Def[_]): Option[(Rep[SPairRDDFunctions[K, V]], Rep[K]) forSome {type K; type V}] = d match {
        case MethodCall(receiver, method, Seq(key, _*), _) if receiver.elem.isInstanceOf[SPairRDDFunctionsElem[_, _, _]] && method.getName == "lookup" =>
          Some((receiver, key)).asInstanceOf[Option[(Rep[SPairRDDFunctions[K, V]], Rep[K]) forSome {type K; type V}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SPairRDDFunctions[K, V]], Rep[K]) forSome {type K; type V}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object groupByKey {
      def unapply(d: Def[_]): Option[Rep[SPairRDDFunctions[K, V]] forSome {type K; type V}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SPairRDDFunctionsElem[_, _, _]] && method.getName == "groupByKey" =>
          Some(receiver).asInstanceOf[Option[Rep[SPairRDDFunctions[K, V]] forSome {type K; type V}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SPairRDDFunctions[K, V]] forSome {type K; type V}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object countByKey {
      def unapply(d: Def[_]): Option[Rep[SPairRDDFunctions[K, V]] forSome {type K; type V}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SPairRDDFunctionsElem[_, _, _]] && method.getName == "countByKey" =>
          Some(receiver).asInstanceOf[Option[Rep[SPairRDDFunctions[K, V]] forSome {type K; type V}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SPairRDDFunctions[K, V]] forSome {type K; type V}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object foldByKey {
      def unapply(d: Def[_]): Option[(Rep[SPairRDDFunctions[K, V]], Rep[V], Rep[((V, V)) => V]) forSome {type K; type V}] = d match {
        case MethodCall(receiver, method, Seq(zeroValue, op, _*), _) if receiver.elem.isInstanceOf[SPairRDDFunctionsElem[_, _, _]] && method.getName == "foldByKey" =>
          Some((receiver, zeroValue, op)).asInstanceOf[Option[(Rep[SPairRDDFunctions[K, V]], Rep[V], Rep[((V, V)) => V]) forSome {type K; type V}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SPairRDDFunctions[K, V]], Rep[V], Rep[((V, V)) => V]) forSome {type K; type V}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object join {
      def unapply(d: Def[_]): Option[(Rep[SPairRDDFunctions[K, V]], Rep[SRDD[(K, W)]]) forSome {type K; type V; type W}] = d match {
        case MethodCall(receiver, method, Seq(other, _*), _) if receiver.elem.isInstanceOf[SPairRDDFunctionsElem[_, _, _]] && method.getName == "join" =>
          Some((receiver, other)).asInstanceOf[Option[(Rep[SPairRDDFunctions[K, V]], Rep[SRDD[(K, W)]]) forSome {type K; type V; type W}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SPairRDDFunctions[K, V]], Rep[SRDD[(K, W)]]) forSome {type K; type V; type W}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object groupWithExt {
      def unapply(d: Def[_]): Option[(Rep[SPairRDDFunctions[K, V]], Rep[SRDD[(K, W)]]) forSome {type K; type V; type W}] = d match {
        case MethodCall(receiver, method, Seq(other, _*), _) if receiver.elem.isInstanceOf[SPairRDDFunctionsElem[_, _, _]] && method.getName == "groupWithExt" =>
          Some((receiver, other)).asInstanceOf[Option[(Rep[SPairRDDFunctions[K, V]], Rep[SRDD[(K, W)]]) forSome {type K; type V; type W}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SPairRDDFunctions[K, V]], Rep[SRDD[(K, W)]]) forSome {type K; type V; type W}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object SPairRDDFunctionsCompanionMethods {
    object apply {
      def unapply(d: Def[_]): Option[Rep[SRDD[(K, V)]] forSome {type K; type V}] = d match {
        case MethodCall(receiver, method, Seq(rdd, _*), _) if receiver.elem.isInstanceOf[SPairRDDFunctionsCompanionElem] && method.getName == "apply" =>
          Some(rdd).asInstanceOf[Option[Rep[SRDD[(K, V)]] forSome {type K; type V}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SRDD[(K, V)]] forSome {type K; type V}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }
  override def rewriteDef[T](d: Def[T]) = d match {
    case nobj @ NewObject(clazz, Seq(arg, _*), neverInvoke) if (clazz.getSimpleName == "SPairRDDFunctions") =>
      arg match {
        case HasViews(source, rddIso: SRDDIso[(a1, a2),(b1, b2)]) => {
          val newArg = source.asRep[SRDD[(a1,a2)]]
          val innerIso = rddIso.innerIso
          val eTo = innerIso.eTo
          innerIso match {
            case newIso: PairIso[a1,a2,b1,b2] => {
              implicit val eA1 = newIso.eA1
              implicit val eA2 = newIso.eA2
              implicit val eB1 = newIso.eB1
              implicit val eB2 = newIso.eB2
              implicit val el = sPairRDDFunctionsElement (eA1, eA2 )
              val newObj = newObjEx (classOf[SPairRDDFunctions[a1, a2]], List (newArg.asRep[Any] ) ).asRep[SPairRDDFunctions[a1, a2]]
              ViewSPairRDDFunctions[a1, a2, b1, b2] (newObj) (newIso)
            }
            case _ => super.rewriteDef(d)
          }
        }
        case _ => super.rewriteDef(d)
      }
    case ExpSPairRDDFunctionsImpl(Def(SPairRDDFunctionsMethods.wrappedValueOfBaseType(HasViews(source, pFuncsIso: SPairRDDFunctionsIso[a1, a2, b1, b2])))) => {
      val newSource = source.asRep[SPairRDDFunctions[a1,a2]]
      implicit val eA1 = pFuncsIso.eA1
      implicit val eA2 = pFuncsIso.eA2
      val innerIso = pFuncsIso.iso
      ViewSPairRDDFunctions(SPairRDDFunctionsImpl(newSource))(innerIso)
    }

    case SPairRDDFunctionsMethods.join(HasViews(source, pFuncsIso: SPairRDDFunctionsIso[a1, a2, b1, b2]), rdd2: RepRDD[(_,_)]) => {
      val newSource = source.asRep[SPairRDDFunctions[a1,a2]]
      val innerIso1 = pFuncsIso.iso
      //val eFrom = innerIso.eFrom.asInstanceOf[PairElem[a1,a2]]
      //val eTo = innerIso.eTo.asInstanceOf[PairElem[b1,b2]]
      implicit val eA1 = innerIso1.eA1
      implicit val eA2 = innerIso1.eA2
      implicit val eB1 = innerIso1.eB1
      implicit val eB2 = innerIso1.eB2
      val eRdd2 = rdd2.elem.eItem //.asInstanceOf[PairElem[a1,c]]
      getIsoByElem(eRdd2) match {
        case innerIso2: PairIso[_,c,_,_] if (innerIso1.iso1 == innerIso2.iso1) => {
          implicit val eC = innerIso2.eA2
          val pIso = SRDDIso(PairIso(innerIso1.iso1, PairIso(innerIso1.iso2, innerIso2.iso2)))
          val newarg2 = SRDDIso(innerIso2).from(rdd2).asRep[SRDD[(a1,c)]]

          val newJoin = newSource.join(newarg2)
          //(SRDDIso(innerIso2).from(rdd2.asRep[SRDD[(a1,_)]])) //asRep[SRDD[(a1, c)]])
          ViewSRDD(newJoin)(pIso)
        }
        case _ => super.rewriteDef(d)
      }
    }
    case _ => super.rewriteDef(d)
  }
}
