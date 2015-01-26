package scalan.spark

import scalan._
import org.apache.spark.rdd.PairRDDFunctions

/** Extra functions available on RDDs of (key, value) pairs */
trait PairRDDs extends Base with BaseTypes { self: PairRDDsDsl =>
  type RepPairRDD[K, V] = Rep[PairRDDFunctions[K, V]]

  trait SPairRDD[K, V] extends BaseTypeEx[PairRDDFunctions[K, V], SPairRDD[K, V]] { self =>
    implicit def eK: Elem[K]
    implicit def eV: Elem[V]

    /** Returns a copy of the RDD partitioned using the specified partitioner. */
    @External def partitionBy(partitioner: Partitioner): RepPairRDD[K, V]

    /** Merges the values for each key using an associative reduce function. */
    @External def reduceByKey(func: Rep[(V, V) => V]): RepPairRDD[K, V]
  }
}

trait PairRDDsDsl extends impl.PairRDDsAbs
trait PairRDDsDslSeq extends impl.PairRDDsSeq {
  trait SeqSPairRDD[A] extends SPairRDDImpl[A] {
  }
}
trait PairRDDsDslExp extends impl.PairRDDsExp