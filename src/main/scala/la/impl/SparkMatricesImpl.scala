package la

import scala.annotation.unchecked.uncheckedVariance
import scalan.OverloadId
import scalan.common.OverloadHack.{Overloaded1, Overloaded2}
import scala.reflect._
import scala.reflect.runtime.universe._
import scalan.common.Default
import scalan.spark._

package impl {

import scalan.meta.ScalanAst.STraitOrClassDef

// Abs -----------------------------------
trait SparkMatricesAbs extends SparkMatrices with SparkDsl {
  self: SparkLADsl =>

  // single proxy for each type family
  implicit def proxySparkAbstractMatrix[A](p: Rep[SparkAbstractMatrix[A]]): SparkAbstractMatrix[A] = {
    proxyOps[SparkAbstractMatrix[A]](p)(scala.reflect.classTag[SparkAbstractMatrix[A]])
  }

  // familyElem
  class SparkAbstractMatrixElem[A, To <: SparkAbstractMatrix[A]](implicit val elem: Elem[A])
    extends AbstractMatrixElem[A, To] {
    override lazy val parent: Option[Elem[_]] = Some(abstractMatrixElement(element[A]))
    override lazy val entityDef: STraitOrClassDef = {
      val module = getModules("SparkMatrices")
      module.entities.find(_.name == "SparkAbstractMatrix").get
    }
    override lazy val tyArgSubst: Map[String, TypeDesc] = {
      Map("A" -> Left(elem))
    }
    override def isEntityType = true
    override lazy val tag = {
      implicit val tagA = elem.tag
      weakTypeTag[SparkAbstractMatrix[A]].asInstanceOf[WeakTypeTag[To]]
    }
    override def convert(x: Rep[Reifiable[_]]) = {
      implicit val eTo: Elem[To] = this
      val conv = fun {x: Rep[SparkAbstractMatrix[A]] => convertSparkAbstractMatrix(x) }
      tryConvert(element[SparkAbstractMatrix[A]], this, x, conv)
    }

    def convertSparkAbstractMatrix(x : Rep[SparkAbstractMatrix[A]]): Rep[To] = {
      assert(x.selfType1 match { case _: SparkAbstractMatrixElem[_, _] => true; case _ => false })
      x.asRep[To]
    }
    override def getDefaultRep: Rep[To] = ???
  }

  implicit def sparkAbstractMatrixElement[A](implicit elem: Elem[A]): Elem[SparkAbstractMatrix[A]] =
    new SparkAbstractMatrixElem[A, SparkAbstractMatrix[A]]

  implicit case object SparkAbstractMatrixCompanionElem extends CompanionElem[SparkAbstractMatrixCompanionAbs] {
    lazy val tag = weakTypeTag[SparkAbstractMatrixCompanionAbs]
    protected def getDefaultRep = SparkAbstractMatrix
  }

  abstract class SparkAbstractMatrixCompanionAbs extends CompanionBase[SparkAbstractMatrixCompanionAbs] with SparkAbstractMatrixCompanion {
    override def toString = "SparkAbstractMatrix"
  }
  def SparkAbstractMatrix: Rep[SparkAbstractMatrixCompanionAbs]
  implicit def proxySparkAbstractMatrixCompanion(p: Rep[SparkAbstractMatrixCompanion]): SparkAbstractMatrixCompanion =
    proxyOps[SparkAbstractMatrixCompanion](p)

  // elem for concrete class
  class SparkSparseIndexedMatrixElem[T](val iso: Iso[SparkSparseIndexedMatrixData[T], SparkSparseIndexedMatrix[T]])(implicit elem: Elem[T])
    extends SparkAbstractMatrixElem[T, SparkSparseIndexedMatrix[T]]
    with ConcreteElem[SparkSparseIndexedMatrixData[T], SparkSparseIndexedMatrix[T]] {
    override lazy val parent: Option[Elem[_]] = Some(sparkAbstractMatrixElement(element[T]))
    override lazy val entityDef = {
      val module = getModules("SparkMatrices")
      module.concreteSClasses.find(_.name == "SparkSparseIndexedMatrix").get
    }
    override lazy val tyArgSubst: Map[String, TypeDesc] = {
      Map("T" -> Left(elem))
    }

    override def convertSparkAbstractMatrix(x: Rep[SparkAbstractMatrix[T]]) = SparkSparseIndexedMatrix(x.rddNonZeroIndexes, x.rddNonZeroValues, x.numColumns)
    override def getDefaultRep = super[ConcreteElem].getDefaultRep
    override lazy val tag = {
      implicit val tagT = elem.tag
      weakTypeTag[SparkSparseIndexedMatrix[T]]
    }
  }

  // state representation type
  type SparkSparseIndexedMatrixData[T] = (RDDIndexedCollection[Array[Int]], (RDDIndexedCollection[Array[T]], Int))

  // 3) Iso for concrete class
  class SparkSparseIndexedMatrixIso[T](implicit elem: Elem[T])
    extends Iso[SparkSparseIndexedMatrixData[T], SparkSparseIndexedMatrix[T]]()(pairElement(implicitly[Elem[RDDIndexedCollection[Array[Int]]]], pairElement(implicitly[Elem[RDDIndexedCollection[Array[T]]]], implicitly[Elem[Int]]))) {
    override def from(p: Rep[SparkSparseIndexedMatrix[T]]) =
      (p.rddNonZeroIndexes, p.rddNonZeroValues, p.numColumns)
    override def to(p: Rep[(RDDIndexedCollection[Array[Int]], (RDDIndexedCollection[Array[T]], Int))]) = {
      val Pair(rddNonZeroIndexes, Pair(rddNonZeroValues, numColumns)) = p
      SparkSparseIndexedMatrix(rddNonZeroIndexes, rddNonZeroValues, numColumns)
    }
    lazy val defaultRepTo: Rep[SparkSparseIndexedMatrix[T]] = SparkSparseIndexedMatrix(element[RDDIndexedCollection[Array[Int]]].defaultRepValue, element[RDDIndexedCollection[Array[T]]].defaultRepValue, 0)
    lazy val eTo = new SparkSparseIndexedMatrixElem[T](this)
  }
  // 4) constructor and deconstructor
  abstract class SparkSparseIndexedMatrixCompanionAbs extends CompanionBase[SparkSparseIndexedMatrixCompanionAbs] with SparkSparseIndexedMatrixCompanion {
    override def toString = "SparkSparseIndexedMatrix"
    def apply[T](p: Rep[SparkSparseIndexedMatrixData[T]])(implicit elem: Elem[T]): Rep[SparkSparseIndexedMatrix[T]] =
      isoSparkSparseIndexedMatrix(elem).to(p)
    def apply[T](rddNonZeroIndexes: RDDIndexColl[Array[Int]], rddNonZeroValues: RDDIndexColl[Array[T]], numColumns: Rep[Int])(implicit elem: Elem[T]): Rep[SparkSparseIndexedMatrix[T]] =
      mkSparkSparseIndexedMatrix(rddNonZeroIndexes, rddNonZeroValues, numColumns)
  }
  object SparkSparseIndexedMatrixMatcher {
    def unapply[T](p: Rep[SparkAbstractMatrix[T]]) = unmkSparkSparseIndexedMatrix(p)
  }
  def SparkSparseIndexedMatrix: Rep[SparkSparseIndexedMatrixCompanionAbs]
  implicit def proxySparkSparseIndexedMatrixCompanion(p: Rep[SparkSparseIndexedMatrixCompanionAbs]): SparkSparseIndexedMatrixCompanionAbs = {
    proxyOps[SparkSparseIndexedMatrixCompanionAbs](p)
  }

  implicit case object SparkSparseIndexedMatrixCompanionElem extends CompanionElem[SparkSparseIndexedMatrixCompanionAbs] {
    lazy val tag = weakTypeTag[SparkSparseIndexedMatrixCompanionAbs]
    protected def getDefaultRep = SparkSparseIndexedMatrix
  }

  implicit def proxySparkSparseIndexedMatrix[T](p: Rep[SparkSparseIndexedMatrix[T]]): SparkSparseIndexedMatrix[T] =
    proxyOps[SparkSparseIndexedMatrix[T]](p)

  implicit class ExtendedSparkSparseIndexedMatrix[T](p: Rep[SparkSparseIndexedMatrix[T]])(implicit elem: Elem[T]) {
    def toData: Rep[SparkSparseIndexedMatrixData[T]] = isoSparkSparseIndexedMatrix(elem).from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoSparkSparseIndexedMatrix[T](implicit elem: Elem[T]): Iso[SparkSparseIndexedMatrixData[T], SparkSparseIndexedMatrix[T]] =
    new SparkSparseIndexedMatrixIso[T]

  // 6) smart constructor and deconstructor
  def mkSparkSparseIndexedMatrix[T](rddNonZeroIndexes: RDDIndexColl[Array[Int]], rddNonZeroValues: RDDIndexColl[Array[T]], numColumns: Rep[Int])(implicit elem: Elem[T]): Rep[SparkSparseIndexedMatrix[T]]
  def unmkSparkSparseIndexedMatrix[T](p: Rep[SparkAbstractMatrix[T]]): Option[(Rep[RDDIndexedCollection[Array[Int]]], Rep[RDDIndexedCollection[Array[T]]], Rep[Int])]

  // elem for concrete class
  class SparkDenseIndexedMatrixElem[T](val iso: Iso[SparkDenseIndexedMatrixData[T], SparkDenseIndexedMatrix[T]])(implicit elem: Elem[T])
    extends SparkAbstractMatrixElem[T, SparkDenseIndexedMatrix[T]]
    with ConcreteElem[SparkDenseIndexedMatrixData[T], SparkDenseIndexedMatrix[T]] {
    override lazy val parent: Option[Elem[_]] = Some(sparkAbstractMatrixElement(element[T]))
    override lazy val entityDef = {
      val module = getModules("SparkMatrices")
      module.concreteSClasses.find(_.name == "SparkDenseIndexedMatrix").get
    }
    override lazy val tyArgSubst: Map[String, TypeDesc] = {
      Map("T" -> Left(elem))
    }

    override def convertSparkAbstractMatrix(x: Rep[SparkAbstractMatrix[T]]) = SparkDenseIndexedMatrix(x.rddValues, x.numColumns)
    override def getDefaultRep = super[ConcreteElem].getDefaultRep
    override lazy val tag = {
      implicit val tagT = elem.tag
      weakTypeTag[SparkDenseIndexedMatrix[T]]
    }
  }

  // state representation type
  type SparkDenseIndexedMatrixData[T] = (RDDIndexedCollection[Array[T]], Int)

  // 3) Iso for concrete class
  class SparkDenseIndexedMatrixIso[T](implicit elem: Elem[T])
    extends Iso[SparkDenseIndexedMatrixData[T], SparkDenseIndexedMatrix[T]]()(pairElement(implicitly[Elem[RDDIndexedCollection[Array[T]]]], implicitly[Elem[Int]])) {
    override def from(p: Rep[SparkDenseIndexedMatrix[T]]) =
      (p.rddValues, p.numColumns)
    override def to(p: Rep[(RDDIndexedCollection[Array[T]], Int)]) = {
      val Pair(rddValues, numColumns) = p
      SparkDenseIndexedMatrix(rddValues, numColumns)
    }
    lazy val defaultRepTo: Rep[SparkDenseIndexedMatrix[T]] = SparkDenseIndexedMatrix(element[RDDIndexedCollection[Array[T]]].defaultRepValue, 0)
    lazy val eTo = new SparkDenseIndexedMatrixElem[T](this)
  }
  // 4) constructor and deconstructor
  abstract class SparkDenseIndexedMatrixCompanionAbs extends CompanionBase[SparkDenseIndexedMatrixCompanionAbs] with SparkDenseIndexedMatrixCompanion {
    override def toString = "SparkDenseIndexedMatrix"
    def apply[T](p: Rep[SparkDenseIndexedMatrixData[T]])(implicit elem: Elem[T]): Rep[SparkDenseIndexedMatrix[T]] =
      isoSparkDenseIndexedMatrix(elem).to(p)
    def apply[T](rddValues: RDDIndexColl[Array[T]], numColumns: Rep[Int])(implicit elem: Elem[T]): Rep[SparkDenseIndexedMatrix[T]] =
      mkSparkDenseIndexedMatrix(rddValues, numColumns)
  }
  object SparkDenseIndexedMatrixMatcher {
    def unapply[T](p: Rep[SparkAbstractMatrix[T]]) = unmkSparkDenseIndexedMatrix(p)
  }
  def SparkDenseIndexedMatrix: Rep[SparkDenseIndexedMatrixCompanionAbs]
  implicit def proxySparkDenseIndexedMatrixCompanion(p: Rep[SparkDenseIndexedMatrixCompanionAbs]): SparkDenseIndexedMatrixCompanionAbs = {
    proxyOps[SparkDenseIndexedMatrixCompanionAbs](p)
  }

  implicit case object SparkDenseIndexedMatrixCompanionElem extends CompanionElem[SparkDenseIndexedMatrixCompanionAbs] {
    lazy val tag = weakTypeTag[SparkDenseIndexedMatrixCompanionAbs]
    protected def getDefaultRep = SparkDenseIndexedMatrix
  }

  implicit def proxySparkDenseIndexedMatrix[T](p: Rep[SparkDenseIndexedMatrix[T]]): SparkDenseIndexedMatrix[T] =
    proxyOps[SparkDenseIndexedMatrix[T]](p)

  implicit class ExtendedSparkDenseIndexedMatrix[T](p: Rep[SparkDenseIndexedMatrix[T]])(implicit elem: Elem[T]) {
    def toData: Rep[SparkDenseIndexedMatrixData[T]] = isoSparkDenseIndexedMatrix(elem).from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoSparkDenseIndexedMatrix[T](implicit elem: Elem[T]): Iso[SparkDenseIndexedMatrixData[T], SparkDenseIndexedMatrix[T]] =
    new SparkDenseIndexedMatrixIso[T]

  // 6) smart constructor and deconstructor
  def mkSparkDenseIndexedMatrix[T](rddValues: RDDIndexColl[Array[T]], numColumns: Rep[Int])(implicit elem: Elem[T]): Rep[SparkDenseIndexedMatrix[T]]
  def unmkSparkDenseIndexedMatrix[T](p: Rep[SparkAbstractMatrix[T]]): Option[(Rep[RDDIndexedCollection[Array[T]]], Rep[Int])]

  registerModule(scalan.meta.ScalanCodegen.loadModule(SparkMatrices_Module.dump))
}

// Seq -----------------------------------
trait SparkMatricesSeq extends SparkMatricesDsl with SparkDslSeq {
  self: SparkLADslSeq =>
  lazy val SparkAbstractMatrix: Rep[SparkAbstractMatrixCompanionAbs] = new SparkAbstractMatrixCompanionAbs with UserTypeSeq[SparkAbstractMatrixCompanionAbs] {
    lazy val selfType = element[SparkAbstractMatrixCompanionAbs]
  }

  case class SeqSparkSparseIndexedMatrix[T]
      (override val rddNonZeroIndexes: RDDIndexColl[Array[Int]], override val rddNonZeroValues: RDDIndexColl[Array[T]], override val numColumns: Rep[Int])
      (implicit elem: Elem[T])
    extends SparkSparseIndexedMatrix[T](rddNonZeroIndexes, rddNonZeroValues, numColumns)
        with UserTypeSeq[SparkSparseIndexedMatrix[T]] {
    lazy val selfType = element[SparkSparseIndexedMatrix[T]]
  }
  lazy val SparkSparseIndexedMatrix = new SparkSparseIndexedMatrixCompanionAbs with UserTypeSeq[SparkSparseIndexedMatrixCompanionAbs] {
    lazy val selfType = element[SparkSparseIndexedMatrixCompanionAbs]
  }

  def mkSparkSparseIndexedMatrix[T]
      (rddNonZeroIndexes: RDDIndexColl[Array[Int]], rddNonZeroValues: RDDIndexColl[Array[T]], numColumns: Rep[Int])(implicit elem: Elem[T]): Rep[SparkSparseIndexedMatrix[T]] =
      new SeqSparkSparseIndexedMatrix[T](rddNonZeroIndexes, rddNonZeroValues, numColumns)
  def unmkSparkSparseIndexedMatrix[T](p: Rep[SparkAbstractMatrix[T]]) = p match {
    case p: SparkSparseIndexedMatrix[T] @unchecked =>
      Some((p.rddNonZeroIndexes, p.rddNonZeroValues, p.numColumns))
    case _ => None
  }

  case class SeqSparkDenseIndexedMatrix[T]
      (override val rddValues: RDDIndexColl[Array[T]], override val numColumns: Rep[Int])
      (implicit elem: Elem[T])
    extends SparkDenseIndexedMatrix[T](rddValues, numColumns)
        with UserTypeSeq[SparkDenseIndexedMatrix[T]] {
    lazy val selfType = element[SparkDenseIndexedMatrix[T]]
  }
  lazy val SparkDenseIndexedMatrix = new SparkDenseIndexedMatrixCompanionAbs with UserTypeSeq[SparkDenseIndexedMatrixCompanionAbs] {
    lazy val selfType = element[SparkDenseIndexedMatrixCompanionAbs]
  }

  def mkSparkDenseIndexedMatrix[T]
      (rddValues: RDDIndexColl[Array[T]], numColumns: Rep[Int])(implicit elem: Elem[T]): Rep[SparkDenseIndexedMatrix[T]] =
      new SeqSparkDenseIndexedMatrix[T](rddValues, numColumns)
  def unmkSparkDenseIndexedMatrix[T](p: Rep[SparkAbstractMatrix[T]]) = p match {
    case p: SparkDenseIndexedMatrix[T] @unchecked =>
      Some((p.rddValues, p.numColumns))
    case _ => None
  }
}

// Exp -----------------------------------
trait SparkMatricesExp extends SparkMatricesDsl with SparkDslExp {
  self: SparkLADslExp =>
  lazy val SparkAbstractMatrix: Rep[SparkAbstractMatrixCompanionAbs] = new SparkAbstractMatrixCompanionAbs with UserTypeDef[SparkAbstractMatrixCompanionAbs] {
    lazy val selfType = element[SparkAbstractMatrixCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  case class ExpSparkSparseIndexedMatrix[T]
      (override val rddNonZeroIndexes: RDDIndexColl[Array[Int]], override val rddNonZeroValues: RDDIndexColl[Array[T]], override val numColumns: Rep[Int])
      (implicit elem: Elem[T])
    extends SparkSparseIndexedMatrix[T](rddNonZeroIndexes, rddNonZeroValues, numColumns) with UserTypeDef[SparkSparseIndexedMatrix[T]] {
    lazy val selfType = element[SparkSparseIndexedMatrix[T]]
    override def mirror(t: Transformer) = ExpSparkSparseIndexedMatrix[T](t(rddNonZeroIndexes), t(rddNonZeroValues), t(numColumns))
  }

  lazy val SparkSparseIndexedMatrix: Rep[SparkSparseIndexedMatrixCompanionAbs] = new SparkSparseIndexedMatrixCompanionAbs with UserTypeDef[SparkSparseIndexedMatrixCompanionAbs] {
    lazy val selfType = element[SparkSparseIndexedMatrixCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object SparkSparseIndexedMatrixMethods {
    object numRows {
      def unapply(d: Def[_]): Option[Rep[SparkSparseIndexedMatrix[T]] forSome {type T}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SparkSparseIndexedMatrixElem[_]] && method.getName == "numRows" =>
          Some(receiver).asInstanceOf[Option[Rep[SparkSparseIndexedMatrix[T]] forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SparkSparseIndexedMatrix[T]] forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object rddColl {
      def unapply(d: Def[_]): Option[Rep[SparkSparseIndexedMatrix[T]] forSome {type T}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SparkSparseIndexedMatrixElem[_]] && method.getName == "rddColl" =>
          Some(receiver).asInstanceOf[Option[Rep[SparkSparseIndexedMatrix[T]] forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SparkSparseIndexedMatrix[T]] forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object sc {
      def unapply(d: Def[_]): Option[Rep[SparkSparseIndexedMatrix[T]] forSome {type T}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SparkSparseIndexedMatrixElem[_]] && method.getName == "sc" =>
          Some(receiver).asInstanceOf[Option[Rep[SparkSparseIndexedMatrix[T]] forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SparkSparseIndexedMatrix[T]] forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object rows {
      def unapply(d: Def[_]): Option[Rep[SparkSparseIndexedMatrix[T]] forSome {type T}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SparkSparseIndexedMatrixElem[_]] && method.getName == "rows" =>
          Some(receiver).asInstanceOf[Option[Rep[SparkSparseIndexedMatrix[T]] forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SparkSparseIndexedMatrix[T]] forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object rmValues {
      def unapply(d: Def[_]): Option[Rep[SparkSparseIndexedMatrix[T]] forSome {type T}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SparkSparseIndexedMatrixElem[_]] && method.getName == "rmValues" =>
          Some(receiver).asInstanceOf[Option[Rep[SparkSparseIndexedMatrix[T]] forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SparkSparseIndexedMatrix[T]] forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object columns {
      def unapply(d: Def[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], Numeric[T]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(n, _*), _) if receiver.elem.isInstanceOf[SparkSparseIndexedMatrixElem[_]] && method.getName == "columns" =>
          Some((receiver, n)).asInstanceOf[Option[(Rep[SparkSparseIndexedMatrix[T]], Numeric[T]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], Numeric[T]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object rddValues {
      def unapply(d: Def[_]): Option[Rep[SparkSparseIndexedMatrix[T]] forSome {type T}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SparkSparseIndexedMatrixElem[_]] && method.getName == "rddValues" =>
          Some(receiver).asInstanceOf[Option[Rep[SparkSparseIndexedMatrix[T]] forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SparkSparseIndexedMatrix[T]] forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object apply_rows {
      def unapply(d: Def[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], Coll[Int]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(iRows, _*), _) if receiver.elem.isInstanceOf[SparkSparseIndexedMatrixElem[_]] && method.getName == "apply" && { val ann = method.getAnnotation(classOf[scalan.OverloadId]); ann != null && ann.value == "rows" } =>
          Some((receiver, iRows)).asInstanceOf[Option[(Rep[SparkSparseIndexedMatrix[T]], Coll[Int]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], Coll[Int]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object apply_row {
      def unapply(d: Def[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], Rep[Int]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(row, _*), _) if receiver.elem.isInstanceOf[SparkSparseIndexedMatrixElem[_]] && method.getName == "apply" && { val ann = method.getAnnotation(classOf[scalan.OverloadId]); ann != null && ann.value == "row" } =>
          Some((receiver, row)).asInstanceOf[Option[(Rep[SparkSparseIndexedMatrix[T]], Rep[Int]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], Rep[Int]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object apply {
      def unapply(d: Def[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], Rep[Int], Rep[Int]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(row, column, _*), _) if receiver.elem.isInstanceOf[SparkSparseIndexedMatrixElem[_]] && method.getName == "apply" && method.getAnnotation(classOf[scalan.OverloadId]) == null =>
          Some((receiver, row, column)).asInstanceOf[Option[(Rep[SparkSparseIndexedMatrix[T]], Rep[Int], Rep[Int]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], Rep[Int], Rep[Int]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object mapBy {
      def unapply(d: Def[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], Rep[AbstractVector[T] => AbstractVector[R] @uncheckedVariance]) forSome {type T; type R}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[SparkSparseIndexedMatrixElem[_]] && method.getName == "mapBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[SparkSparseIndexedMatrix[T]], Rep[AbstractVector[T] => AbstractVector[R] @uncheckedVariance]) forSome {type T; type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], Rep[AbstractVector[T] => AbstractVector[R] @uncheckedVariance]) forSome {type T; type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object transpose {
      def unapply(d: Def[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], Numeric[T]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(n, _*), _) if receiver.elem.isInstanceOf[SparkSparseIndexedMatrixElem[_]] && method.getName == "transpose" =>
          Some((receiver, n)).asInstanceOf[Option[(Rep[SparkSparseIndexedMatrix[T]], Numeric[T]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], Numeric[T]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object reduceByRows {
      def unapply(d: Def[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], RepMonoid[T]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(m, _*), _) if receiver.elem.isInstanceOf[SparkSparseIndexedMatrixElem[_]] && method.getName == "reduceByRows" =>
          Some((receiver, m)).asInstanceOf[Option[(Rep[SparkSparseIndexedMatrix[T]], RepMonoid[T]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], RepMonoid[T]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object countNonZeroesByColumns {
      def unapply(d: Def[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], Numeric[T]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(n, _*), _) if receiver.elem.isInstanceOf[SparkSparseIndexedMatrixElem[_]] && method.getName == "countNonZeroesByColumns" =>
          Some((receiver, n)).asInstanceOf[Option[(Rep[SparkSparseIndexedMatrix[T]], Numeric[T]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], Numeric[T]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object reduceByColumns {
      def unapply(d: Def[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], RepMonoid[T], Numeric[T]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(m, n, _*), _) if receiver.elem.isInstanceOf[SparkSparseIndexedMatrixElem[_]] && method.getName == "reduceByColumns" =>
          Some((receiver, m, n)).asInstanceOf[Option[(Rep[SparkSparseIndexedMatrix[T]], RepMonoid[T], Numeric[T]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], RepMonoid[T], Numeric[T]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object * {
      def unapply(d: Def[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], Vector[T], Numeric[T]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(vector, n, _*), _) if receiver.elem.isInstanceOf[SparkSparseIndexedMatrixElem[_]] && method.getName == "$times" && method.getAnnotation(classOf[scalan.OverloadId]) == null =>
          Some((receiver, vector, n)).asInstanceOf[Option[(Rep[SparkSparseIndexedMatrix[T]], Vector[T], Numeric[T]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], Vector[T], Numeric[T]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object matrix_* {
      def unapply(d: Def[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], Matrix[T], Numeric[T]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(matrix, n, _*), _) if receiver.elem.isInstanceOf[SparkSparseIndexedMatrixElem[_]] && method.getName == "$times" && { val ann = method.getAnnotation(classOf[scalan.OverloadId]); ann != null && ann.value == "matrix" } =>
          Some((receiver, matrix, n)).asInstanceOf[Option[(Rep[SparkSparseIndexedMatrix[T]], Matrix[T], Numeric[T]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], Matrix[T], Numeric[T]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object matrix_+^^ {
      def unapply(d: Def[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], Rep[AbstractMatrix[T]], Numeric[T]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(other, n, _*), _) if receiver.elem.isInstanceOf[SparkSparseIndexedMatrixElem[_]] && method.getName == "$plus$up$up" =>
          Some((receiver, other, n)).asInstanceOf[Option[(Rep[SparkSparseIndexedMatrix[T]], Rep[AbstractMatrix[T]], Numeric[T]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], Rep[AbstractMatrix[T]], Numeric[T]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object matrix_*^^ {
      def unapply(d: Def[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], Rep[AbstractMatrix[T]], Numeric[T]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(other, n, _*), _) if receiver.elem.isInstanceOf[SparkSparseIndexedMatrixElem[_]] && method.getName == "$times$up$up" && { val ann = method.getAnnotation(classOf[scalan.OverloadId]); ann != null && ann.value == "matrix" } =>
          Some((receiver, other, n)).asInstanceOf[Option[(Rep[SparkSparseIndexedMatrix[T]], Rep[AbstractMatrix[T]], Numeric[T]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], Rep[AbstractMatrix[T]], Numeric[T]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object *^^ {
      def unapply(d: Def[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], Rep[T], Numeric[T]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(value, n, _*), _) if receiver.elem.isInstanceOf[SparkSparseIndexedMatrixElem[_]] && method.getName == "$times$up$up" && method.getAnnotation(classOf[scalan.OverloadId]) == null =>
          Some((receiver, value, n)).asInstanceOf[Option[(Rep[SparkSparseIndexedMatrix[T]], Rep[T], Numeric[T]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], Rep[T], Numeric[T]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object average {
      def unapply(d: Def[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], Fractional[T], RepMonoid[T]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(f, m, _*), _) if receiver.elem.isInstanceOf[SparkSparseIndexedMatrixElem[_]] && method.getName == "average" =>
          Some((receiver, f, m)).asInstanceOf[Option[(Rep[SparkSparseIndexedMatrix[T]], Fractional[T], RepMonoid[T]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkSparseIndexedMatrix[T]], Fractional[T], RepMonoid[T]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object companion {
      def unapply(d: Def[_]): Option[Rep[SparkSparseIndexedMatrix[T]] forSome {type T}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SparkSparseIndexedMatrixElem[_]] && method.getName == "companion" =>
          Some(receiver).asInstanceOf[Option[Rep[SparkSparseIndexedMatrix[T]] forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SparkSparseIndexedMatrix[T]] forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object SparkSparseIndexedMatrixCompanionMethods {
  }

  def mkSparkSparseIndexedMatrix[T]
    (rddNonZeroIndexes: RDDIndexColl[Array[Int]], rddNonZeroValues: RDDIndexColl[Array[T]], numColumns: Rep[Int])(implicit elem: Elem[T]): Rep[SparkSparseIndexedMatrix[T]] =
    new ExpSparkSparseIndexedMatrix[T](rddNonZeroIndexes, rddNonZeroValues, numColumns)
  def unmkSparkSparseIndexedMatrix[T](p: Rep[SparkAbstractMatrix[T]]) = p.elem.asInstanceOf[Elem[_]] match {
    case _: SparkSparseIndexedMatrixElem[T] @unchecked =>
      Some((p.asRep[SparkSparseIndexedMatrix[T]].rddNonZeroIndexes, p.asRep[SparkSparseIndexedMatrix[T]].rddNonZeroValues, p.asRep[SparkSparseIndexedMatrix[T]].numColumns))
    case _ =>
      None
  }

  case class ExpSparkDenseIndexedMatrix[T]
      (override val rddValues: RDDIndexColl[Array[T]], override val numColumns: Rep[Int])
      (implicit elem: Elem[T])
    extends SparkDenseIndexedMatrix[T](rddValues, numColumns) with UserTypeDef[SparkDenseIndexedMatrix[T]] {
    lazy val selfType = element[SparkDenseIndexedMatrix[T]]
    override def mirror(t: Transformer) = ExpSparkDenseIndexedMatrix[T](t(rddValues), t(numColumns))
  }

  lazy val SparkDenseIndexedMatrix: Rep[SparkDenseIndexedMatrixCompanionAbs] = new SparkDenseIndexedMatrixCompanionAbs with UserTypeDef[SparkDenseIndexedMatrixCompanionAbs] {
    lazy val selfType = element[SparkDenseIndexedMatrixCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object SparkDenseIndexedMatrixMethods {
    object numRows {
      def unapply(d: Def[_]): Option[Rep[SparkDenseIndexedMatrix[T]] forSome {type T}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SparkDenseIndexedMatrixElem[_]] && method.getName == "numRows" =>
          Some(receiver).asInstanceOf[Option[Rep[SparkDenseIndexedMatrix[T]] forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SparkDenseIndexedMatrix[T]] forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object sc {
      def unapply(d: Def[_]): Option[Rep[SparkDenseIndexedMatrix[T]] forSome {type T}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SparkDenseIndexedMatrixElem[_]] && method.getName == "sc" =>
          Some(receiver).asInstanceOf[Option[Rep[SparkDenseIndexedMatrix[T]] forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SparkDenseIndexedMatrix[T]] forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object rows {
      def unapply(d: Def[_]): Option[Rep[SparkDenseIndexedMatrix[T]] forSome {type T}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SparkDenseIndexedMatrixElem[_]] && method.getName == "rows" =>
          Some(receiver).asInstanceOf[Option[Rep[SparkDenseIndexedMatrix[T]] forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SparkDenseIndexedMatrix[T]] forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object rmValues {
      def unapply(d: Def[_]): Option[Rep[SparkDenseIndexedMatrix[T]] forSome {type T}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SparkDenseIndexedMatrixElem[_]] && method.getName == "rmValues" =>
          Some(receiver).asInstanceOf[Option[Rep[SparkDenseIndexedMatrix[T]] forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SparkDenseIndexedMatrix[T]] forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object columns {
      def unapply(d: Def[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], Numeric[T]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(n, _*), _) if receiver.elem.isInstanceOf[SparkDenseIndexedMatrixElem[_]] && method.getName == "columns" =>
          Some((receiver, n)).asInstanceOf[Option[(Rep[SparkDenseIndexedMatrix[T]], Numeric[T]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], Numeric[T]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object rddNonZeroIndexes {
      def unapply(d: Def[_]): Option[Rep[SparkDenseIndexedMatrix[T]] forSome {type T}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SparkDenseIndexedMatrixElem[_]] && method.getName == "rddNonZeroIndexes" =>
          Some(receiver).asInstanceOf[Option[Rep[SparkDenseIndexedMatrix[T]] forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SparkDenseIndexedMatrix[T]] forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object rddNonZeroValues {
      def unapply(d: Def[_]): Option[Rep[SparkDenseIndexedMatrix[T]] forSome {type T}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SparkDenseIndexedMatrixElem[_]] && method.getName == "rddNonZeroValues" =>
          Some(receiver).asInstanceOf[Option[Rep[SparkDenseIndexedMatrix[T]] forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SparkDenseIndexedMatrix[T]] forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object apply_rows {
      def unapply(d: Def[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], Coll[Int]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(iRows, _*), _) if receiver.elem.isInstanceOf[SparkDenseIndexedMatrixElem[_]] && method.getName == "apply" && { val ann = method.getAnnotation(classOf[scalan.OverloadId]); ann != null && ann.value == "rows" } =>
          Some((receiver, iRows)).asInstanceOf[Option[(Rep[SparkDenseIndexedMatrix[T]], Coll[Int]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], Coll[Int]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object apply_row {
      def unapply(d: Def[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], Rep[Int]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(row, _*), _) if receiver.elem.isInstanceOf[SparkDenseIndexedMatrixElem[_]] && method.getName == "apply" && { val ann = method.getAnnotation(classOf[scalan.OverloadId]); ann != null && ann.value == "row" } =>
          Some((receiver, row)).asInstanceOf[Option[(Rep[SparkDenseIndexedMatrix[T]], Rep[Int]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], Rep[Int]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object apply {
      def unapply(d: Def[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], Rep[Int], Rep[Int]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(row, column, _*), _) if receiver.elem.isInstanceOf[SparkDenseIndexedMatrixElem[_]] && method.getName == "apply" && method.getAnnotation(classOf[scalan.OverloadId]) == null =>
          Some((receiver, row, column)).asInstanceOf[Option[(Rep[SparkDenseIndexedMatrix[T]], Rep[Int], Rep[Int]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], Rep[Int], Rep[Int]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object mapBy {
      def unapply(d: Def[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], Rep[AbstractVector[T] => AbstractVector[R] @uncheckedVariance]) forSome {type T; type R}] = d match {
        case MethodCall(receiver, method, Seq(f, _*), _) if receiver.elem.isInstanceOf[SparkDenseIndexedMatrixElem[_]] && method.getName == "mapBy" =>
          Some((receiver, f)).asInstanceOf[Option[(Rep[SparkDenseIndexedMatrix[T]], Rep[AbstractVector[T] => AbstractVector[R] @uncheckedVariance]) forSome {type T; type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], Rep[AbstractVector[T] => AbstractVector[R] @uncheckedVariance]) forSome {type T; type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object transpose {
      def unapply(d: Def[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], Numeric[T]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(n, _*), _) if receiver.elem.isInstanceOf[SparkDenseIndexedMatrixElem[_]] && method.getName == "transpose" =>
          Some((receiver, n)).asInstanceOf[Option[(Rep[SparkDenseIndexedMatrix[T]], Numeric[T]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], Numeric[T]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object reduceByRows {
      def unapply(d: Def[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], RepMonoid[T]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(m, _*), _) if receiver.elem.isInstanceOf[SparkDenseIndexedMatrixElem[_]] && method.getName == "reduceByRows" =>
          Some((receiver, m)).asInstanceOf[Option[(Rep[SparkDenseIndexedMatrix[T]], RepMonoid[T]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], RepMonoid[T]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object countNonZeroesByColumns {
      def unapply(d: Def[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], Numeric[T]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(n, _*), _) if receiver.elem.isInstanceOf[SparkDenseIndexedMatrixElem[_]] && method.getName == "countNonZeroesByColumns" =>
          Some((receiver, n)).asInstanceOf[Option[(Rep[SparkDenseIndexedMatrix[T]], Numeric[T]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], Numeric[T]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object reduceByColumns {
      def unapply(d: Def[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], RepMonoid[T], Numeric[T]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(m, n, _*), _) if receiver.elem.isInstanceOf[SparkDenseIndexedMatrixElem[_]] && method.getName == "reduceByColumns" =>
          Some((receiver, m, n)).asInstanceOf[Option[(Rep[SparkDenseIndexedMatrix[T]], RepMonoid[T], Numeric[T]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], RepMonoid[T], Numeric[T]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object * {
      def unapply(d: Def[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], Vector[T], Numeric[T]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(vector, n, _*), _) if receiver.elem.isInstanceOf[SparkDenseIndexedMatrixElem[_]] && method.getName == "$times" && method.getAnnotation(classOf[scalan.OverloadId]) == null =>
          Some((receiver, vector, n)).asInstanceOf[Option[(Rep[SparkDenseIndexedMatrix[T]], Vector[T], Numeric[T]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], Vector[T], Numeric[T]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object matrix_* {
      def unapply(d: Def[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], Matrix[T], Numeric[T]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(matrix, n, _*), _) if receiver.elem.isInstanceOf[SparkDenseIndexedMatrixElem[_]] && method.getName == "$times" && { val ann = method.getAnnotation(classOf[scalan.OverloadId]); ann != null && ann.value == "matrix" } =>
          Some((receiver, matrix, n)).asInstanceOf[Option[(Rep[SparkDenseIndexedMatrix[T]], Matrix[T], Numeric[T]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], Matrix[T], Numeric[T]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object matrix_+^^ {
      def unapply(d: Def[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], Rep[AbstractMatrix[T]], Numeric[T]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(other, n, _*), _) if receiver.elem.isInstanceOf[SparkDenseIndexedMatrixElem[_]] && method.getName == "$plus$up$up" =>
          Some((receiver, other, n)).asInstanceOf[Option[(Rep[SparkDenseIndexedMatrix[T]], Rep[AbstractMatrix[T]], Numeric[T]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], Rep[AbstractMatrix[T]], Numeric[T]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object matrix_*^^ {
      def unapply(d: Def[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], Rep[AbstractMatrix[T]], Numeric[T]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(other, n, _*), _) if receiver.elem.isInstanceOf[SparkDenseIndexedMatrixElem[_]] && method.getName == "$times$up$up" && { val ann = method.getAnnotation(classOf[scalan.OverloadId]); ann != null && ann.value == "matrix" } =>
          Some((receiver, other, n)).asInstanceOf[Option[(Rep[SparkDenseIndexedMatrix[T]], Rep[AbstractMatrix[T]], Numeric[T]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], Rep[AbstractMatrix[T]], Numeric[T]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object *^^ {
      def unapply(d: Def[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], Rep[T], Numeric[T]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(value, n, _*), _) if receiver.elem.isInstanceOf[SparkDenseIndexedMatrixElem[_]] && method.getName == "$times$up$up" && method.getAnnotation(classOf[scalan.OverloadId]) == null =>
          Some((receiver, value, n)).asInstanceOf[Option[(Rep[SparkDenseIndexedMatrix[T]], Rep[T], Numeric[T]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], Rep[T], Numeric[T]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object average {
      def unapply(d: Def[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], Fractional[T], RepMonoid[T]) forSome {type T}] = d match {
        case MethodCall(receiver, method, Seq(f, m, _*), _) if receiver.elem.isInstanceOf[SparkDenseIndexedMatrixElem[_]] && method.getName == "average" =>
          Some((receiver, f, m)).asInstanceOf[Option[(Rep[SparkDenseIndexedMatrix[T]], Fractional[T], RepMonoid[T]) forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkDenseIndexedMatrix[T]], Fractional[T], RepMonoid[T]) forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object companion {
      def unapply(d: Def[_]): Option[Rep[SparkDenseIndexedMatrix[T]] forSome {type T}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SparkDenseIndexedMatrixElem[_]] && method.getName == "companion" =>
          Some(receiver).asInstanceOf[Option[Rep[SparkDenseIndexedMatrix[T]] forSome {type T}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SparkDenseIndexedMatrix[T]] forSome {type T}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object SparkDenseIndexedMatrixCompanionMethods {
  }

  def mkSparkDenseIndexedMatrix[T]
    (rddValues: RDDIndexColl[Array[T]], numColumns: Rep[Int])(implicit elem: Elem[T]): Rep[SparkDenseIndexedMatrix[T]] =
    new ExpSparkDenseIndexedMatrix[T](rddValues, numColumns)
  def unmkSparkDenseIndexedMatrix[T](p: Rep[SparkAbstractMatrix[T]]) = p.elem.asInstanceOf[Elem[_]] match {
    case _: SparkDenseIndexedMatrixElem[T] @unchecked =>
      Some((p.asRep[SparkDenseIndexedMatrix[T]].rddValues, p.asRep[SparkDenseIndexedMatrix[T]].numColumns))
    case _ =>
      None
  }

  object SparkAbstractMatrixMethods {
    object numColumns {
      def unapply(d: Def[_]): Option[Rep[SparkAbstractMatrix[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SparkAbstractMatrixElem[_, _]] && method.getName == "numColumns" =>
          Some(receiver).asInstanceOf[Option[Rep[SparkAbstractMatrix[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SparkAbstractMatrix[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object rddNonZeroIndexes {
      def unapply(d: Def[_]): Option[Rep[SparkAbstractMatrix[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SparkAbstractMatrixElem[_, _]] && method.getName == "rddNonZeroIndexes" =>
          Some(receiver).asInstanceOf[Option[Rep[SparkAbstractMatrix[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SparkAbstractMatrix[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object rddNonZeroValues {
      def unapply(d: Def[_]): Option[Rep[SparkAbstractMatrix[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SparkAbstractMatrixElem[_, _]] && method.getName == "rddNonZeroValues" =>
          Some(receiver).asInstanceOf[Option[Rep[SparkAbstractMatrix[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SparkAbstractMatrix[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object rddValues {
      def unapply(d: Def[_]): Option[Rep[SparkAbstractMatrix[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SparkAbstractMatrixElem[_, _]] && method.getName == "rddValues" =>
          Some(receiver).asInstanceOf[Option[Rep[SparkAbstractMatrix[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SparkAbstractMatrix[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object sc {
      def unapply(d: Def[_]): Option[Rep[SparkAbstractMatrix[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SparkAbstractMatrixElem[_, _]] && method.getName == "sc" =>
          Some(receiver).asInstanceOf[Option[Rep[SparkAbstractMatrix[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SparkAbstractMatrix[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object zeroValue {
      def unapply(d: Def[_]): Option[Rep[SparkAbstractMatrix[A]] forSome {type A}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[SparkAbstractMatrixElem[_, _]] && method.getName == "zeroValue" =>
          Some(receiver).asInstanceOf[Option[Rep[SparkAbstractMatrix[A]] forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[SparkAbstractMatrix[A]] forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object columns {
      def unapply(d: Def[_]): Option[(Rep[SparkAbstractMatrix[A]], Numeric[A]) forSome {type A}] = d match {
        case MethodCall(receiver, method, Seq(n, _*), _) if receiver.elem.isInstanceOf[SparkAbstractMatrixElem[_, _]] && method.getName == "columns" =>
          Some((receiver, n)).asInstanceOf[Option[(Rep[SparkAbstractMatrix[A]], Numeric[A]) forSome {type A}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[SparkAbstractMatrix[A]], Numeric[A]) forSome {type A}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object SparkAbstractMatrixCompanionMethods {
  }
}

object SparkMatrices_Module {
  val packageName = "la"
  val name = "SparkMatrices"
  val dump = "H4sIAAAAAAAAAM1WTWwbRRSeXcdxbIc0LdCmUiPSYIpAEAckVKQcKtd2UJDzQzYgZCqk8e4k3XZ3djMzjtYcekCc4Ia4ItR7b1yQkHpBSIgDJwRInDmVIlS19ATizeyP18brJKBK7GG0Ozvzfr7ve2/m1l2U5wxd4CZ2MF1yicBLhnqvcVExmlTYorfuWV2HNMju+6e/MNfpZa6jE200eRXzBnfaqBi+NAM/eTfIfgsVMTUJFx7jAp1vKQ9V03McYgrbo1XbdbsCdxxSbdlcrLTQRMezevvoBtJaaNb0qMmIIEbdwZwTHs1PERmRnXwX1Xdv0+/7oFWZRTWVxQ7DtoDwwcdsuH6b+EaPerTnCjQThbbpy7BgTcF2fY+J2EUBzF31rPhzgmKYQKda1/ABroKLvaohmE33YGfZx+Z1vEc2YIlcPgEBc+Ls7vR89Z1roRIn+wDQmus7aibwEULAwMsqiKU+PksJPksSn4pBmI0d+z0sf24xL+ih8NFyCAU+mHjhEBOxBdKkVuXDK+Y7D42yq8vNgQyloDKcBENPZahBUQE4frP9Mb/32s2LOiq1UcnmtQ4XDJsiTXmEVhlT6gkVcwIgZnvA1mIWW8pLDdYMSaJoeq6PKViKoJwGnhzbtIVcLOemI3YyoC8In8RLtcDXknwXMvJVuqljx9m6c/bFZ35tvq0jfdBFEUwaIHwWGxXoccPH7HoMyToGbQSRJzmeEEjbUXDLoRj0x8KYSBJMnr3zm/X1MrqiJ0hGjo9GHpjI859+KH//3CUdTbWV1FcdvNcGMHnTIe4mq3tUtNGUd0BY+KdwgB35NpLMgkV2cdcREcRpbHKAjUALmUXpEwnciioALQagHGp4w6OksrpV+cP49pNbUqIMTYd/wir9y774588zu0KpV6CTzLJgS5swb41aJCA8xnp6u9FQU3WoikEa8jXGcC/B7eksEfhki9kuNJ0D8spXX775++2NvNLBqSj1t7DTJWELiDLvoyCD05YFyq1RkeZbeZ1LEpfDvECz/TSU0WNnkS2w0Q5LtOuCya5LE1c5aI2hNTlcGCXSUsiE4bnk5OI9+92bHwklRy0YbIubnWvQh1bUvnNjlBm35wftZf3+2R8/01ERBNixhYv9yvIRm8ojbBRoEN2ZenQ0qYp6afDnnKp+OXASStEKW0CKkXjtmcEmUU9HnSKv0GcM1HQ+y8HA9vl+Q38yFeCcNiSACQI1H8cz0Yw/xinpkBznk4Kazy4oAPf0dusJ5+6l2zrKv47yu9BfeAvlO16XWjFrcAUQJBCX4zltkDVgCTPsJiypZwH10x8KXy0sa4MJ/odu/Q9wh6urCOX8/61jOb6qxpXjCPyMAqxB6CPS90KG/X8hbzmsHVfc4/KbT+1/41C9pcCYHI3zWNnVRsvuMPLKso+tYtd2ekPMjZL6GB8xIedGbBtBRsbN52iwjc71g/6aaOFjyoOyLE8EpINAQygYWsxoN0bU2+GAufHw043nv/v8F3V8l+QpAdcMmlz008f2IHQl5bdVg4t7KlIQlTw4VJR/A7He6wpIDQAA"
}
}

