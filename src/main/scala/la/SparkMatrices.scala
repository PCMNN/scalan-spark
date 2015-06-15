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
    def sc: Rep[SSparkContext] = rddIdxs.rdd.context
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
      val idxs: Rep[SRDD[Array[Int]]] = rddIdxs.rdd
      val vals: Rep[SRDD[Array[T]]] = rddVals.rdd

      val rs: Rep[SRDD[Int]] = sc.makeRDD(SSeq(SArray.rangeFrom0(numRows)))
      val cols: Rep[SRDD[Int]] = sc.makeRDD(SSeq(SArray.rangeFrom0(numColumns)))

      val flatValsWithRows: Rep[SRDD[(Int, T)]] = (rs zip vals).flatMap({ rv: Rep[(Int, Array[T])] => SSeq(rv._2.map { v: Rep[T] => (rv._1,v)}) } )
      val flatIdxs: Rep[SRDD[Int]] = idxs.flatMap({ a: Rep[Array[Int]] => SSeq(a) })
      //val flatValsWithRows = valsWithRow.flatMap(a=>a)
      val zippedFlat: Rep[SRDD[(Int, (Int, T))]] = flatIdxs zip flatValsWithRows

      type SIT = SSeq[(Int,T)]
      //val transp: Rep[SRDD[(Int,SIT)]] = SPairRDDFunctions(zippedFlat).groupByKey

      val empty: Rep[Int] = -1 //SSeq.empty[(Int,T)]
      //val empty: Rep[SRDD[SSeq[(Int,T)]]] = sc.makeRDD(SSeq(SArray.replicate(numColumns, emptySeq)))
      val eCols: Rep[SPairRDDFunctionsImpl[Int, Int]] =  SPairRDDFunctionsImpl(SPairRDDFunctions(cols.map (fun{c => (c, empty)})))


      val r = eCols.groupWithExt(zippedFlat)

      val el: LElem[(Int,(SSeq[Int], SSeq[(Int,T)]))] = toLazyElem(PairElem(element[Int], PairElem(element[SSeq[Int]], element[SSeq[(Int,T)]])))
      val result = r.map(fun{in: Rep[(Int, (SSeq[Int], SSeq[(Int,T)]))] =>
        val Pair(_,Pair(_,seq)) = in
        seq.toArray
      }(el))

      /*val r: Rep[SRDD[(Int, (SSeq[SSeq[(Int, T)]], SSeq[SSeq[(Int,T)]]))]] = SPairRDDFunctions(eCols).groupWithExt(transp)

      type SIT = SSeq[(Int,T)]
      val el: LElem[(Int,(SSeq[SIT], SSeq[SIT]))] = toLazyElem(PairElem(element[Int], PairElem(element[SSeq[SIT]], element[SSeq[SIT]])))

      val result: Rep[SRDD[Array[(Int,T)]]] = r.map( fun{in: Rep[(Int, (SSeq[SIT], SSeq[SIT]))] =>
        val Pair(_,Pair(s1: Rep[SSeq[SIT]],s2: Rep[SSeq[SIT]])) = in
        val union: Rep[SSeq[SIT]] = (s1 ++ s2)

        implicit val el1: Elem[(SIT, SIT)] = PairElem(element[SIT], element[SIT])

        val reduceFun: Rep[((SIT, SIT)) => SIT] = fun[(SIT, SIT), SIT]{ seqs: Rep[(SIT, SIT)] =>
          val Pair(seq1: Rep[SIT], seq2: Rep[SIT]) = seqs
          seq1 ++ seq2
        }

        val reduced: Rep[SSeq[(Int,T)]] = union.reduce(reduceFun)
        reduced.toArray
      }(el)
      ) */
      val resIdxs: Rep[RDDCollection[Array[Int]]] = RDDCollection(result.map(fun{ in: Rep[Array[(Int,T)]] => in.map{ i => i._1}}))
      val resVals: Rep[RDDCollection[Array[T]]] = RDDCollection(result.map(fun{ in: Rep[Array[(Int,T)]] => in.map{ i => i._2}}))

      SparkSparseMatrix(resIdxs, resVals, numRows)

      //val r2 = r1.map({in => in._2.reduce(in(a,b) => a++b)})

      //val flatIdxs = rddIdxs.flatMap({ i: Rep[Array[Int]] => Collection(i)}).convertTo[RDDCollection[Int]].rdd
      //val flatVals = rddVals.flatMap({ i: Rep[Array[T]] => Collection(i)} ).convertTo[RDDCollection[T]].rdd
      //val transposed = rddToPairRddFunctions(flatIdxs zip flatVals).combineByKey
      /*val idxs = rddIdxs.rdd
      val vals = rddVals.rdd
      val zipped = (idxs zip vals).zipWithIndex.map({in: Rep[((Array[Int], Array[Double]), Long)] =>
        (in._1._1, (in._2.toInt, in._1._2))
      })

      ???*/

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

  abstract class SparkSparseIndexedMatrix[T] (val rddIdxs: Rep[RDDIndexedCollection[Array[Int]]], val rddVals: Rep[RDDIndexedCollection[Array[T]]], val numColumns: Rep[Int])(implicit val elem: Elem[T]) extends SparkAbstractMatrix[T] {
    def numRows: Rep[Int] = rddIdxs.length
    def rddColl  = rddIdxs zip rddVals
    def sc: Rep[SSparkContext] = rddIdxs.indexedRdd.context
    def rows = rddColl.map({arrs: Rep[(Array[Int], Array[T])] => SparseVector(Collection(arrs._1), Collection(arrs._2), numColumns)})
    def rmValues: Rep[Collection[T]] = ???

    @OverloadId("rows")
    def apply(iRows: Coll[Int])(implicit o: Overloaded1): SparkMatrix[T] = SparkSparseIndexedMatrix(rddIdxs(iRows).convertTo[RDDIndexedCollection[Array[Int]]], rddVals(iRows).convertTo[RDDIndexedCollection[Array[T]]] , numColumns)
    @OverloadId("row")
    def apply(row: Rep[Int]): Vector[T] = ???
    def apply(row: Rep[Int], column: Rep[Int]): Rep[T] = ???
    def mapBy[R: Elem](f: Rep[AbstractVector[T] => AbstractVector[R] @uncheckedVariance]): Matrix[R] = ???
    def columns(implicit n: Numeric[T]): Rep[Collection[AbstractVector[T]]] = ???

    def transpose(implicit n: Numeric[T]): SparkMatrix[T] = {
      val idxs: Rep[SRDD[Array[Int]]] = rddIdxs.rdd
      val vals: Rep[SRDD[Array[T]]] = rddVals.rdd

      val rs: Rep[SRDD[Int]] = sc.makeRDD(SSeq(SArray.rangeFrom0(numRows)))
      val cols: Rep[SRDD[Int]] = sc.makeRDD(SSeq(SArray.rangeFrom0(numColumns)))

      val flatValsWithRows: Rep[SRDD[(Int, T)]] = (rs zip vals).flatMap({ rv: Rep[(Int, Array[T])] => SSeq(rv._2.map { v: Rep[T] => (rv._1,v)}) } )
      val flatIdxs: Rep[SRDD[Int]] = idxs.flatMap({ a: Rep[Array[Int]] => SSeq(a) })
      //val flatValsWithRows = valsWithRow.flatMap(a=>a)
      val zippedFlat: Rep[SRDD[(Int, (Int, T))]] = flatIdxs zip flatValsWithRows

      val empty: Rep[Int] = -1 //SSeq.empty[(Int,T)]
      val eCols: Rep[SPairRDDFunctionsImpl[Int, Int]] =  SPairRDDFunctionsImpl(SPairRDDFunctions(cols.map (fun{c => (c, empty)})))


      val r = eCols.groupWithExt(zippedFlat)

      val el: LElem[(Int,(SSeq[Int], SSeq[(Int,T)]))] = toLazyElem(PairElem(element[Int], PairElem(element[SSeq[Int]], element[SSeq[(Int,T)]])))
      val result = r.map(fun{in: Rep[(Int, (SSeq[Int], SSeq[(Int,T)]))] =>
        val Pair(_,Pair(_,seq)) = in
        seq.toArray
      }(el))

      val resIdxs: Rep[RDDCollection[Array[Int]]] = RDDCollection(result.map(fun{ in: Rep[Array[(Int,T)]] => in.map{ i => i._1}}))
      val resVals: Rep[RDDCollection[Array[T]]] = RDDCollection(result.map(fun{ in: Rep[Array[(Int,T)]] => in.map{ i => i._2}}))

      SparkSparseMatrix(resIdxs, resVals, numRows)
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
      val flatIdxs = rddIdxs.indexedRdd.flatMap({ i: Rep[(Long,Array[Int])] => SSeq(i._2)} )
      val flatVals = rddVals.indexedRdd.flatMap({ i: Rep[(Long,Array[T])] => SSeq(i._2)} )
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

  trait SparkSparseIndexedMatrixCompanion extends ConcreteClass1[SparkSparseIndexedMatrix] with AbstractMatrixCompanion {

  }

  abstract class SparkDenseMatrix[T] (val rddVals: Rep[RDDCollection[Array[T]]], val numColumns: Rep[Int])(implicit val elem: Elem[T]) extends SparkAbstractMatrix[T] {
    def numRows: Rep[Int] = rddVals.length
    def sc = rddVals.rdd.context
    def rows = rddVals.map({arr: Rep[Array[T]] => DenseVector(Collection(arr))})
    def rmValues: Rep[Collection[T]] = ???

    @OverloadId("rows")
    def apply(iRows: Coll[Int])(implicit o: Overloaded1): SparkMatrix[T] = SparkDenseMatrix(rddVals(iRows).convertTo[RDDCollection[Array[T]]] , numColumns)
    @OverloadId("row")
    def apply(row: Rep[Int]): Vector[T] = ???
    def apply(row: Rep[Int], column: Rep[Int]): Rep[T] = ???
    def mapBy[R: Elem](f: Rep[AbstractVector[T] => AbstractVector[R] @uncheckedVariance]): Matrix[R] = ???
    def columns(implicit n: Numeric[T]): Rep[Collection[AbstractVector[T]]] = ???

    def transpose(implicit n: Numeric[T]): SparkMatrix[T] = {  ???
      /*
      val flatIdxs = rddIdxs.flatMap({ i: Rep[Array[Int]] => Collection(i)}).convertTo[RDDCollection[Int]].rdd
      val flatVals = rddVals.flatMap({ i: Rep[Array[T]] => Collection(i)} ).convertTo[RDDCollection[T]].rdd
      val transposed = rddToPairRddFunctions(flatIdxs zip flatVals).combineByKey
      ???
      *?*/
    }
    override def reduceByRows(implicit m: RepMonoid[T]): Vector[T] = {
      DenseVector(rddVals.map({arr => arr.reduce(m)}))
    }

    override def countNonZeroesByColumns(implicit n: Numeric[T]): Vector[Int] = { ???
      /*val flatIdxs = rddIdxs.rdd.flatMap({ i: Rep[Array[Int]] => SSeq(i)} )
      val flatVals = rddVals.rdd.flatMap({ i: Rep[Array[T]] => SSeq(i)} )
      val cMap = SPairRDDFunctions(flatIdxs zip flatVals).countByKey
      DenseVector(Collection.indexRange(numColumns).map({k => cMap.applyIfBy(k, fun{ i:Rep[Long] => i.toInt}, fun { _: Rep[Unit] => toRep(0)}) }) )
      */
    }

    override def reduceByColumns(implicit m: RepMonoid[T], n: Numeric[T]): Vector[T] = {
      val newZero = SArray.replicate(numColumns,m.zero)
      val newFun =  fun {in: Rep[(Array[T],Array[T])] =>
        (in._1 zip in._2).map{ pair => m.append(pair._1, pair._2)}
      }
      val newArr = rddVals.rdd.fold(newZero)( newFun )
      DenseVector(Collection(newArr))
    }

    override def *(vector: Vector[T])(implicit n: Numeric[T]): Vector[T] =
      DenseVector(rows.map { r => r.dot(vector) })
    @OverloadId("matrix")
    def *(matrix: Matrix[T])(implicit n: Numeric[T], o: Overloaded1): Matrix[T] = ???


    @OverloadId("matrix")
    def +^^(other: Rep[AbstractMatrix[T]])(implicit n: Numeric[T]): Rep[AbstractMatrix[T]] = {
      val newRows = (rows zip other.rows).map({vecs: Rep[(AbstractVector[T], AbstractVector[T])] => (vecs._1 +^ vecs._2).items.arr})
      SparkDenseMatrix(newRows.asRep[RDDCollection[Array[T]]], numColumns)
    }
    def *^^(other: Rep[AbstractMatrix[T]])(implicit n: Numeric[T]): Rep[AbstractMatrix[T]] = {
      val newRows = (rows zip other.rows).map({vecs: Rep[(AbstractVector[T], AbstractVector[T])] => (vecs._1 *^ vecs._2).items.arr})
      SparkDenseMatrix(newRows.asRep[RDDCollection[Array[T]]], numColumns)
    }

    def average(implicit f: Fractional[T], m: RepMonoid[T]): DoubleRep = {
      val items = rows.flatMap(v => v.nonZeroValues)
      items.reduce.toDouble / items.length.toDouble
    }

    def companion: Rep[AbstractMatrixCompanion] = SparkDenseMatrix
  }

  trait SparkDenseMatrixCompanion extends ConcreteClass1[SparkDenseMatrix] with AbstractMatrixCompanion {

  }

  abstract class SparkDenseIndexedMatrix[T] (val rddVals: Rep[RDDIndexedCollection[Array[T]]], val numColumns: Rep[Int])(implicit val elem: Elem[T]) extends SparkAbstractMatrix[T] {
    def numRows: Rep[Int] = rddVals.length
    def sc = rddVals.rdd.context
    def rows = rddVals.map({arr: Rep[Array[T]] => DenseVector(Collection(arr))})
    def rmValues: Rep[Collection[T]] = ???

    @OverloadId("rows")
    def apply(iRows: Coll[Int])(implicit o: Overloaded1): SparkMatrix[T] = SparkDenseIndexedMatrix(rddVals(iRows).convertTo[RDDIndexedCollection[Array[T]]] , numColumns)
    @OverloadId("row")
    def apply(row: Rep[Int]): Vector[T] = ???
    def apply(row: Rep[Int], column: Rep[Int]): Rep[T] = ???
    def mapBy[R: Elem](f: Rep[AbstractVector[T] => AbstractVector[R] @uncheckedVariance]): Matrix[R] = ???
    def columns(implicit n: Numeric[T]): Rep[Collection[AbstractVector[T]]] = ???

    def transpose(implicit n: Numeric[T]): SparkMatrix[T] = {  ???
      /*
      val flatIdxs = rddIdxs.flatMap({ i: Rep[Array[Int]] => Collection(i)}).convertTo[RDDCollection[Int]].rdd
      val flatVals = rddVals.flatMap({ i: Rep[Array[T]] => Collection(i)} ).convertTo[RDDCollection[T]].rdd
      val transposed = rddToPairRddFunctions(flatIdxs zip flatVals).combineByKey
      ???
      *?*/
    }
    override def reduceByRows(implicit m: RepMonoid[T]): Vector[T] = {
      DenseVector(rddVals.map({arr => arr.reduce(m)}))
    }

    override def countNonZeroesByColumns(implicit n: Numeric[T]): Vector[Int] = { ???
      /*val flatIdxs = rddIdxs.rdd.flatMap({ i: Rep[Array[Int]] => SSeq(i)} )
      val flatVals = rddVals.rdd.flatMap({ i: Rep[Array[T]] => SSeq(i)} )
      val cMap = SPairRDDFunctions(flatIdxs zip flatVals).countByKey
      DenseVector(Collection.indexRange(numColumns).map({k => cMap.applyIfBy(k, fun{ i:Rep[Long] => i.toInt}, fun { _: Rep[Unit] => toRep(0)}) }) )
      */
    }

    override def reduceByColumns(implicit m: RepMonoid[T], n: Numeric[T]): Vector[T] = {
      val newZero = SArray.replicate(numColumns,m.zero)
      val newFun =  fun {in: Rep[(Array[T],Array[T])] =>
        (in._1 zip in._2).map{ pair => m.append(pair._1, pair._2)}
      }
      val newArr = rddVals.rdd.fold(newZero)( newFun )
      DenseVector(Collection(newArr))
    }

    override def *(vector: Vector[T])(implicit n: Numeric[T]): Vector[T] =
      DenseVector(rows.map { r => r.dot(vector) })
    @OverloadId("matrix")
    def *(matrix: Matrix[T])(implicit n: Numeric[T], o: Overloaded1): Matrix[T] = ???


    @OverloadId("matrix")
    def +^^(other: Rep[AbstractMatrix[T]])(implicit n: Numeric[T]): Rep[AbstractMatrix[T]] = ??? /*{
      val newRows = (rows zip other.rows).map({vecs: Rep[(AbstractVector[T], AbstractVector[T])] => (vecs._1 +^ vecs._2).items.arr})
      SparkDenseMatrix(newRows.asRep[RDDCollection[Array[T]]], numColumns)
    }*/
    def *^^(other: Rep[AbstractMatrix[T]])(implicit n: Numeric[T]): Rep[AbstractMatrix[T]] = ??? /*{
      val newRows = (rows zip other.rows).map({vecs: Rep[(AbstractVector[T], AbstractVector[T])] => (vecs._1 *^ vecs._2).items.arr})
      SparkDenseMatrix(newRows.asRep[RDDCollection[Array[T]]], numColumns)
    }  */

    def average(implicit f: Fractional[T], m: RepMonoid[T]): DoubleRep = {
      val items = rows.flatMap(v => v.nonZeroValues)
      items.reduce.toDouble / items.length.toDouble
    }

    def companion: Rep[AbstractMatrixCompanion] = SparkDenseIndexedMatrix
  }

  trait SparkDenseIndexedMatrixCompanion extends ConcreteClass1[SparkDenseIndexedMatrix] with AbstractMatrixCompanion {

  }
}

trait SparkMatricesDsl extends impl.SparkMatricesAbs { self: SparkLADsl => }

trait SparkMatricesDslSeq extends impl.SparkMatricesSeq { self: SparkLADslSeq =>
}

trait SparkMatricesDslExp extends impl.SparkMatricesExp { self: SparkLADslExp => }
