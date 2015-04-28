package la

import scala.annotation.unchecked.uncheckedVariance
import scalan.OverloadId
import scalan.common.OverloadHack.{Overloaded1, Overloaded2}

/**
 * Created by afilippov on 4/20/15.
 */
trait SparkMatrices {  self: SparkLADsl =>
  type SparkMatrix[A] = Rep[SparkAbstractMatrix[A]]
  trait SparkAbstractMatrix[A] extends AbstractMatrix[A] {
    implicit def elem: Elem[A]
    val numColumns: Rep[Int]
    def sc: Rep[SSparkContext]
  }
  trait SparkAbstractMatrixCompanion extends TypeFamily1[SparkAbstractMatrix] {
  }

  abstract class SparkSparseMatrix[T] (val rddIdxs: Rep[RDDCollection[Array[Int]]], val rddVals: Rep[RDDCollection[Array[T]]], val numColumns: Rep[Int])(implicit val elem: Elem[T]) extends SparkAbstractMatrix[T] {
    def numRows: Rep[Int] = rddIdxs.length
    def rddColl  = rddIdxs zip rddVals
    def sc = rddIdxs.rdd.context
    def rows = rddColl.map({arrs: Rep[(Array[Int], Array[T])] => SparseVector(Collection(arrs._1), Collection(arrs._2), numColumns)})
    def rmValues: Rep[Collection[T]] = ???

    @OverloadId("rows")
    def apply(iRows: Coll[Int])(implicit o: Overloaded1): SparkMatrix[T] = SparkSparseMatrix(rddIdxs(iRows).convertTo[RDDCollection[Array[Int]]], rddVals(iRows).convertTo[RDDCollection[Array[T]]] , numColumns)
    @OverloadId("row")
    def apply(row: Rep[Int]): Vector[T] = ???
    def apply(row: Rep[Int], column: Rep[Int]): Rep[T] = ???
    def mapBy[R: Elem](f: Rep[AbstractVector[T] => AbstractVector[R] @uncheckedVariance]): Matrix[R] = ???
    def columns(implicit n: Numeric[T]): Rep[Collection[AbstractVector[T]]] = ???

    def transpose(implicit n: Numeric[T]): SparkMatrix[T] = {
      val flatIdxs = rddIdxs.flatMap({ i: Rep[Array[Int]] => Collection(i)}).convertTo[RDDCollection[Int]].rdd
      val flatVals = rddVals.flatMap({ i: Rep[Array[T]] => Collection(i)} ).convertTo[RDDCollection[T]].rdd
      val transposed = rddToPairRddFunctions(flatIdxs zip flatVals).combineByKey
      ???

    }
    override def reduceByRows(implicit m: RepMonoid[T]): Vector[T] = {
      DenseVector(rddVals.map({arr => arr.reduce(m)}))
    }

    override def countNonZeroesByColumns(implicit n: Numeric[T]): Vector[Int] = {
      val flatIdxs = rddIdxs.rdd.flatMap({ i: Rep[Array[Int]] => SSeq(i)} )
      val flatVals = rddVals.rdd.flatMap({ i: Rep[Array[T]] => SSeq(i)} )
      val cMap = SPairRDDFunctions(flatIdxs zip flatVals).countByKey
      DenseVector(Collection.indexRange(numColumns).map({k => cMap.applyIfBy(k, fun{ i:Rep[Long] => i.toInt}, fun { _: Rep[Unit] => toRep(0)}) }) )
    }

    override def reduceByColumns(implicit m: RepMonoid[T], n: Numeric[T]): Vector[T] = {
      val flatIdxs = rddIdxs.rdd.flatMap({ i: Rep[Array[Int]] => SSeq(i)} )
      val flatVals = rddVals.rdd.flatMap({ i: Rep[Array[T]] => SSeq(i)} )
      val red = SPairRDDFunctions(flatIdxs zip flatVals).foldByKey(m.zero)( fun {in: Rep[(T,T)] => m.append(in._1, in._2)} )
      val cMap = mapFromArray(red.collect)
      DenseVector(Collection.indexRange(numColumns).map({k => cMap.applyIfBy(k, fun{ i:Rep[T] => i}, fun { _: Rep[Unit] => m.zero}) }) )
    }

    override def *(vector: Vector[T])(implicit n: Numeric[T]): Vector[T] =
      DenseVector(rows.map { r => r.dot(vector) })
    @OverloadId("matrix")
    def *(matrix: Matrix[T])(implicit n: Numeric[T], o: Overloaded1): Matrix[T] = ???


    @OverloadId("matrix")
    def +^^(other: Rep[AbstractMatrix[T]])(implicit n: Numeric[T]): Rep[AbstractMatrix[T]] = ???
    def *^^(other: Rep[AbstractMatrix[T]])(implicit n: Numeric[T]): Rep[AbstractMatrix[T]] = ???
    def average(implicit f: Fractional[T], m: RepMonoid[T]): DoubleRep = {
      val items = rows.flatMap(v => v.nonZeroValues)
      items.reduce.toDouble / items.length.toDouble
    }

    def companion: Rep[AbstractMatrixCompanion] = SparkSparseMatrix
  }

  trait SparkSparseMatrixCompanion extends ConcreteClass1[SparkSparseMatrix] with AbstractMatrixCompanion {

  }
}

trait SparkMatricesDsl extends impl.SparkMatricesAbs { self: SparkLADsl => }

trait SparkMatricesDslSeq extends impl.SparkMatricesSeq { self: SparkLADslSeq =>
}

trait SparkMatricesDslExp extends impl.SparkMatricesExp { self: SparkLADslExp => }
