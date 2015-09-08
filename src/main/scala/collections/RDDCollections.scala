package scalan.spark.collections

/**
 * Created by afilippov on 3/27/15.
 */
import scala.annotation.unchecked.uncheckedVariance
import scalan.OverloadId
import scalan.common.Default
import scalan.common.OverloadHack.Overloaded1
import scalan.spark.{SparkDslExp, SparkDslSeq, SparkDsl}

/**
 * Created by afilippov on 3/24/15.
 */
trait RDDCollections { self: SparkDsl with RDDCollectionsDsl =>
  type RDDColl[A] = Rep[IRDDCollection[A]]
  type RDDIndexColl[A] = Rep[RDDIndexedCollection[A]]
  trait IRDDCollection[A] extends Collection[A] {
    implicit def eItem: Elem[A]
    def rdd: RepRDD[A]
    def indexedRdd: RepRDD[(Long,A)]
  }

  trait IRDDCollectionCompanion extends TypeFamily1[IRDDCollection] {
  }


  abstract class RDDCollection[A](val rdd: RepRDD[A])(implicit val eItem: Elem[A]) extends IRDDCollection[A] {
    lazy val elem = element[A]
    def indexedRdd = rdd.zipWithIndex.map({p: Rep[(A,Long)] => Pair(p._2, p._1)})
    def arr = rdd.collect
    def lst = arr.toList
    def apply(i: Rep[Int]) = {
      rddToPairRddFunctions(indexedRdd).lookup(i.toLong)(0)
    }
    def length = rdd.count.toInt
    def slice(offset: Rep[Int], length: Rep[Int]) = {
      val indexKey: RepRDD[(A, Long)] = rdd.zipWithIndex.filter(fun{p: Rep[(A,Long)] =>
        val ind: Rep[Int] = p._2.toInt
        (ind >= offset) && (ind < offset + length) })
      RDDCollection(indexKey.map({p: Rep[(A,Long)]} => p._1))
    }
    @OverloadId("many")
    def apply(indices: Coll[Int])(implicit o: Overloaded1): RDDColl[A] = {
      val irdd = SRDD.fromArraySC(rdd.context, indices.arr).map(fun {(i: Rep[Int]) => Pair(i.toLong, 0)})
      val vrdd: RepPairRDDFunctions[Long, A] = indexedRdd
      val joinedRdd: RepRDD[(Long, (A, Int))] = SPairRDDFunctionsImpl(vrdd).join(irdd)
      implicit val ppElem = PairElem(element[Long], PairElem(elem, element[Int]))
      RDDCollection( joinedRdd.map(fun {(in: Rep[(Long, (A, Int))]) => in._2}) )
    }
    def mapBy[B: Elem](f: Rep[A @uncheckedVariance => B]): Coll[B] = RDDCollection(rdd.map(f))
    def reduce(implicit m: RepMonoid[A @uncheckedVariance]): Rep[A] = rdd.fold(m.zero)( fun {in: Rep[(A,A)] => m.append(in._1, in._2)} )
    def zip[B: Elem](ys: Coll[B]): PairColl[A,B] = PairRDDCollection(rdd zip SRDD.fromArraySC(rdd.context, ys.arr) )
    def update (idx: Rep[Int], value: Rep[A]): Coll[A] = ??? //PCollection(parr <<- (idx, value))
    def updateMany (idxs: Coll[Int], vals: Coll[A]): Coll[A] = ??? //PCollection(parr <<- (PArray.fromArray(idxs.arr), PArray.fromArray(vals.arr)))
    def filterBy(f: Rep[A @uncheckedVariance => Boolean]): Coll[A] = ???
    def flatMapBy[B: Elem](f: Rep[A @uncheckedVariance => Collection[B]]): Coll[B] = RDDCollection(rdd.flatMap({in: Rep[A] => SSeq(f(in).arr)}))
    def append(value: Rep[A @uncheckedVariance]): Coll[A]  = ??? //PCollection(PArray.replicate(length+1, value) <<- (parr.indices, parr))
    def sortBy[O: Elem](by: Rep[A => O])(implicit o: Ordering[O]): Coll[A] = ???
  }
  trait RDDCollectionCompanion extends ConcreteClass1[RDDCollection] with IRDDCollectionCompanion {
  }

  abstract class RDDIndexedCollection[A](val indexedRdd: RepRDD[(Long,A)])(implicit val eItem: Elem[A]) extends IRDDCollection[A] {
    lazy val elem = element[A]
    def indices = indexedRdd.map( fun { in => in._1})
    def rdd = indexedRdd.map( fun { in => in._2})
    def arr = rdd.collect
    def lst = arr.toList
    def apply(i: Rep[Int]) = {
      rddToPairRddFunctions(indexedRdd).lookup(i.toLong)(0)
    }
    def length = indexedRdd.count.toInt
    def slice(offset: Rep[Int], length: Rep[Int]) = {
      val indexKey: RepRDD[(Long,A)] = indexedRdd.filter(fun{p: Rep[(Long,A)] =>
        val ind: Rep[Int] = p._1.toInt
        (ind >= offset) && (ind < offset + length) })
      RDDIndexedCollection(indexKey)
    }
    @OverloadId("many")
    def apply(indices: Coll[Int])(implicit o: Overloaded1): RDDColl[A] = {
      val irdd = SRDD.fromArraySC(rdd.context, indices.arr).map(fun {(i: Rep[Int]) => Pair(i.toLong, 0)})
      val joinedRdd: RepRDD[(Long, (A, Int))] = rddToPairRddFunctions(indexedRdd).join(irdd)
      implicit val ppElem = PairElem(element[Long], PairElem(elem, element[Int]))
      RDDIndexedCollection( joinedRdd.map(fun {(in: Rep[(Long, (A, Int))]) => (in._1,in._2)}) )
    }
    def mapBy[B: Elem](f: Rep[A @uncheckedVariance => B]): Rep[RDDIndexedCollection[B]] = RDDIndexedCollection(indices zip rdd.map(f))
    def reduce(implicit m: RepMonoid[A @uncheckedVariance]): Rep[A] = rdd.fold(m.zero)( fun {in: Rep[(A,A)] => m.append(in._1, in._2)} )
    def zip[B: Elem](ys: Coll[B]): Rep[PairRDDIndexedCollection[A,B]] = PairRDDIndexedCollection(indices, (rdd zip SRDD.fromArraySC(rdd.context, ys.arr) ))
    def update (idx: Rep[Int], value: Rep[A]): Coll[A] = ??? //PCollection(parr <<- (idx, value))
    def updateMany (idxs: Coll[Int], vals: Coll[A]): Coll[A] = ??? //PCollection(parr <<- (PArray.fromArray(idxs.arr), PArray.fromArray(vals.arr)))
    def filterBy(f: Rep[A @uncheckedVariance => Boolean]): Coll[A] = ???
    def flatMapBy[B: Elem](f: Rep[A @uncheckedVariance => Collection[B]]): Coll[B] = ??? /*RDDIndexedCollection(indexedRdd.flatMap({in: Rep[(Long,A)] =>
      val arr =
      SSeq(f(in._2).arr)}*/
    def append(value: Rep[A @uncheckedVariance]): Coll[A]  = ??? //PCollection(PArray.replicate(length+1, value) <<- (parr.indices, parr))
    def sortBy[O: Elem](by: Rep[A => O])(implicit o: Ordering[O]): Coll[A] = Collection(arr).sortBy(by) // TODO: This is temporary solution
  }
  trait RDDIndexedCollectionCompanion extends ConcreteClass1[RDDIndexedCollection] with IRDDCollectionCompanion {
  }

  trait IRDDPairCollection[A,B] extends PairCollection[A,B] {
    implicit def eA: Elem[A]
    implicit def eB: Elem[B]
    lazy val eItem = element[(A, B)]
    def pairRDD: RepRDD[(A,B)]
    def coll = RDDCollection(pairRDD)
    def innerJoin[C, R](other: PairColl[A, C], f: Rep[((B, C)) => R])(implicit ordK: Ordering[A], eR: Elem[R], eB: Elem[B], eC: Elem[C]) = ???
    def outerJoin[C, R](other: PairColl[A, C], f: Rep[((B, C)) => R], f1: Rep[B => R], f2: Rep[C => R])(implicit ordK: Ordering[A], eR: Elem[R], eB: Elem[B], eC: Elem[C]) = ???
  }

  abstract class PairRDDCollection[A, B](val pairRDD: RepRDD[(A,B)])(implicit val eA: Elem[A], val eB: Elem[B])
    extends IRDDPairCollection[A, B] {
    lazy val elem = element[(A, B)]
    def as = RDDCollection(rddToPairRddFunctions(pairRDD).keys)
    def bs = RDDCollection(rddToPairRddFunctions(pairRDD).values)
    def arr = pairRDD.collect
    def lst = arr.toList
    def apply(i: Rep[Int]) = {
      implicit val ppElem = PairElem(element[Long], elem)
      implicit val lElem = toLazyElem(PairElem(elem, element[Long]))
      val indexKey: RepRDD[(Long, (A,B))] = pairRDD.zipWithIndex.map(fun({p: Rep[((A,B),Long)] => Pair(p._2, p._1)}))
      rddToPairRddFunctions(indexKey).lookup(i.toLong)(0)
    }
    def length = pairRDD.count.toInt
    def slice(offset: Rep[Int], length: Rep[Int]) = {
      implicit val ppElem1 = PairElem(elem, element[Long])
      val indexKey: RepRDD[((A,B), Long)] = pairRDD.zipWithIndex.filter(fun{p: Rep[((A,B),Long)] =>
        val ind: Rep[Int] = p._2.toInt
        (ind >= offset) && (ind < offset + length) })
      RDDCollection(indexKey.map(fun({p: Rep[((A,B),Long)]} => p._1)))
    }
    @OverloadId("many")
    def apply(indices: Coll[Int])(implicit o: Overloaded1): PairColl[A,B] = {
      implicit val ppElem = PairElem(element[Long], elem)
      val lElem = toLazyElem(PairElem(elem, element[Long]))

      val irdd: RepPairRDDFunctions[Long, Int] = SRDD.fromArraySC(pairRDD.context, indices.arr).map(fun {(i: Rep[Int]) => Pair(i.toLong, 0)})

      val vrdd: RepRDD[(Long, (A,B))] = pairRDD.zipWithIndex.map(fun({p: Rep[((A,B),Long)] => Pair(p._2, p._1)})(lElem, ppElem))(ppElem)

      val joinedRdd: RepRDD[(Long, (Int, (A,B)))] = SPairRDDFunctionsImpl(irdd).join(vrdd)

      val lElem1 = toLazyElem(PairElem(element[Long], PairElem(element[Int], elem)))
      PairRDDCollection( joinedRdd.map(fun {(in: Rep[(Long, (Int, (A,B)))]) => Pair(in._3, in._4)} (lElem1, elem)) )
    }
    def mapBy[C: Elem](f: Rep[(A,B) @uncheckedVariance => C]): Coll[C] = RDDCollection(pairRDD.map(f))
    def reduce(implicit m: RepMonoid[(A,B) @uncheckedVariance]): Rep[(A,B)] = {
      val lElem = toLazyElem(PairElem(elem, elem))
      pairRDD.fold(m.zero)(fun { in: Rep[((A, B), (A, B))] => m.append(in._1, (in._2, in._3))}(lElem, elem))
    }
    def zip[C: Elem](ys: Coll[C]): PairColl[(A,B),C] = PairRDDCollection(pairRDD zip SRDD.fromArraySC(pairRDD.context, ys.arr) )
    def update (idx: Rep[Int], value: Rep[(A,B)]): Coll[(A,B)] = ??? //PCollection(parr <<- (idx, value))
    def updateMany (idxs: Coll[Int], vals: Coll[(A,B)]): Coll[(A,B)] = ??? //PCollection(parr <<- (PArray.fromArray(idxs.arr), PArray.fromArray(vals.arr)))
    def filterBy(f: Rep[(A,B) @uncheckedVariance => Boolean]): PairColl[A,B] = ???
    def flatMapBy[C: Elem](f: Rep[(A,B) @uncheckedVariance => Collection[C]]): Coll[C] = ???
    def append(value: Rep[(A,B) @uncheckedVariance]): Coll[(A,B)]  = ??? //PCollection(PArray.replicate(length+1, value) <<- (parr.indices, parr))
    def sortBy[O: Elem](by: Rep[((A,B)) => O])(implicit o: Ordering[O]): Coll[(A,B)] = ???
  }

  trait PairRDDCollectionCompanion extends ConcreteClass2[PairRDDCollection] {
  }

  abstract class PairRDDIndexedCollection[A, B](val indices: RepRDD[Long], val pairRDD: RepRDD[(A,B)])(implicit val eA: Elem[A], val eB: Elem[B])
    extends IRDDPairCollection[A, B] {
    lazy val elem = element[(A, B)]
    def as = RDDIndexedCollection(indices zip rddToPairRddFunctions(pairRDD).keys)
    def bs = RDDIndexedCollection(indices zip rddToPairRddFunctions(pairRDD).values)
    def arr = pairRDD.collect
    def lst = arr.toList
    def apply(i: Rep[Int]) = {
      val ppElem = PairElem(element[Long], elem)
      val lElem = toLazyElem(PairElem(elem, element[Long]))
      val indexKey: RepRDD[(Long, (A,B))] = indices zip pairRDD
      rddToPairRddFunctions(indexKey).lookup(i.toLong)(0)
    }
    def length = pairRDD.count.toInt
    def slice(offset: Rep[Int], length: Rep[Int]) = {
      implicit val ppElem1 = PairElem(element[Long], elem)
      val indexKey: RepRDD[(Long,(A,B))] = (indices zip pairRDD).filter(fun{p: Rep[(Long,(A,B))] =>
        val ind: Rep[Int] = p._1.toInt
        (ind >= offset) && (ind < offset + length) })
      RDDIndexedCollection(indexKey)
    }
    @OverloadId("many")
    def apply(idxs: Coll[Int])(implicit o: Overloaded1): PairColl[A,B] = {
      implicit val ppElem = PairElem(element[Long], elem)
      val lElem = toLazyElem(PairElem(elem, element[Long]))

      val irdd = SRDD.fromArraySC(pairRDD.context, idxs.arr).map(fun {(i: Rep[Int]) => Pair(i.toLong, 0)})

      val joinedRdd: RepRDD[(Long, (Int, (A,B)))] = rddToPairRddFunctions(irdd).join(indices zip pairRDD)

      val lElem1 = toLazyElem(PairElem(element[Long], PairElem(element[Int], elem)))
      val newIndices = joinedRdd.map(fun { in:Rep[(Long, (Int, (A,B)))] => in._1}(lElem1, element[Long]))
      val newPairRDD = joinedRdd.map(fun { in:Rep[(Long, (Int, (A,B)))] => (in._3, in._4)}(lElem1, elem))
      PairRDDIndexedCollection( newIndices, newPairRDD)
    }
    def mapBy[C: Elem](f: Rep[(A,B) @uncheckedVariance => C]): Coll[C] = RDDIndexedCollection(indices zip pairRDD.map(f))
    def reduce(implicit m: RepMonoid[(A,B) @uncheckedVariance]): Rep[(A,B)] = {
      val lElem = toLazyElem(PairElem(elem, elem))
      pairRDD.fold(m.zero)(fun { in: Rep[((A, B), (A, B))] => m.append(in._1, (in._2, in._3))}(lElem, elem))
    }
    def zip[C: Elem](ys: Coll[C]): PairColl[(A,B),C] = PairRDDIndexedCollection(indices, pairRDD zip SRDD.fromArraySC(pairRDD.context, ys.arr) )
    def update (idx: Rep[Int], value: Rep[(A,B)]): Coll[(A,B)] = ??? //PCollection(parr <<- (idx, value))
    def updateMany (idxs: Coll[Int], vals: Coll[(A,B)]): Coll[(A,B)] = ??? //PCollection(parr <<- (PArray.fromArray(idxs.arr), PArray.fromArray(vals.arr)))
    def filterBy(f: Rep[(A,B) @uncheckedVariance => Boolean]): PairColl[A,B] = ???
    def flatMapBy[C: Elem](f: Rep[(A,B) @uncheckedVariance => Collection[C]]): Coll[C] = ???
    def append(value: Rep[(A,B) @uncheckedVariance]): Coll[(A,B)]  = ??? //PCollection(PArray.replicate(length+1, value) <<- (parr.indices, parr))
    def sortBy[O: Elem](by: Rep[((A,B)) => O])(implicit o: Ordering[O]): Coll[(A,B)] = ???
  }

  trait PairRDDIndexedCollectionCompanion extends ConcreteClass2[PairRDDIndexedCollection] {
  }


  trait IRDDNestedCollection[A] extends NestedCollection[A] {
    implicit def eA: Elem[A]
    def values: Rep[IRDDCollection[A]]
    def segments: Rep[PairRDDCollection[Int, Int]]
    lazy val eItem = element[RDDCollection[A]].asElem[Collection[A]]
  }

  abstract class RDDNestedCollection[A](val values: Rep[IRDDCollection[A]], val segments: Rep[PairRDDCollection[Int, Int]])(implicit val eA: Elem[A])
    extends IRDDNestedCollection[A] {
    lazy val elem : Elem[Collection[A]]= element[RDDCollection[A]].asElem[Collection[A]]
    override def segOffsets = segments.as
    override def segLens = segments.bs

    def length = segments.length
    def apply(i: Rep[Int]) = {
      values.slice(segOffsets(i), segLens(i))
    }

    def arr = array_rangeFrom0(length).map { i: Rep[Int] => apply(i)}
    def lst = arr.toList
    def slice(offset: Rep[Int], length: Rep[Int]) = ??? //PNestedCollection(narr.slice(offset, length))
    @OverloadId("many")
    def apply(indices: Coll[Int])(implicit o: Overloaded1): Rep[IRDDNestedCollection[A]] = {
      val rddInd = SRDD.fromArray(indices.arr)
      val flatInds = rddInd.flatMap(fun({ i: Rep[Int] =>
        val offs = segOffsets(i)
        val len = segLens(i)
        val arr = array_rangeFrom0(len).map(x => x + offs)
        SSeq(arr)
      }))
      val newVals = values(RDDCollection(flatInds)).convertTo[IRDDCollection[A]]
      val newSegments = segments // TODO: fixme! This is not correct!
      RDDNestedCollection(newVals, newSegments)
    }

    def mapBy[B: Elem](f: Rep[Collection[A @uncheckedVariance] => B @uncheckedVariance]): Coll[B] = ???
    def reduce(implicit m: RepMonoid[Collection[A @uncheckedVariance]]): Coll[A] = ??? //PCollection(narr.reduce(m))
    def zip[B: Elem](ys: Coll[B]): PairColl[Collection[A],B] = ??? //PCollection(PArray.fromArray(arr)) zip PCollection(PArray.fromArray(ys.arr))
    def update (idx: Rep[Int], value: Rep[Collection[A]]): Rep[IRDDNestedCollection[A]] = ??? //PNestedCollection(narr <<- (idx, PArray.fromArray(value.arr)))
    def updateMany (idxs: Coll[Int], vals: Coll[Collection[A]]): Rep[IRDDNestedCollection[A]] = ???
    def filterBy(f: Rep[Collection[A @uncheckedVariance] => Boolean]): Rep[IRDDNestedCollection[A]] = ???
    def flatMapBy[B: Elem](f: Rep[Collection[A @uncheckedVariance] => Collection[B]]): Coll[B] = ???
    def append(value: Rep[Collection[A @uncheckedVariance]]): Rep[IRDDNestedCollection[A]]  = ??? //Collection(arr.append(value))
    def sortBy[O: Elem](by: Rep[Collection[A @uncheckedVariance] => O])(implicit o: Ordering[O]): Rep[IRDDNestedCollection[A]] = ???
  }

  trait RDDNestedCollectionCompanion extends ConcreteClass1[RDDNestedCollection] {
    def createRDDNestedCollection[A:Elem](vals: Rep[RDDCollection[A]], segments: Rep[PairRDDCollection[Int,Int]]) = RDDNestedCollection(vals,segments)
  }
}
trait RDDCollectionsDsl extends impl.RDDCollectionsAbs { self: SparkDsl => }

trait RDDCollectionsDslSeq extends impl.RDDCollectionsSeq { self: SparkDslSeq =>
}

trait RDDCollectionsDslExp extends impl.RDDCollectionsExp { self: SparkDslExp => }


