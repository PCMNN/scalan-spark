package scalan.spark

import org.apache.spark.rdd._
import scalan._
import scalan.common.Default
import org.apache.spark.Partitioner

trait PairRDDFunctionss extends Base with BaseTypes { self: SparkDsl =>
  type RepPairRDDFunctions[K, V] = Rep[SPairRDDFunctions[K, V]]

  /** Extra functions available on RDDs of (key, value) pairs */
  trait SPairRDDFunctions[K, V] extends BaseTypeEx[PairRDDFunctions[K, V], SPairRDDFunctions[K, V]] { self =>
    implicit def eK: Elem[K]
    implicit def eV: Elem[V]
    def wrappedValueOfBaseType: Rep[PairRDDFunctions[K, V]]

    /** Return an RDD with the keys of each tuple. */
    @External def keys: Rep[SRDD[K]]

    /** Returns an RDD with the values of each tuple. */
    @External def values: Rep[SRDD[V]]

    /** Returns a copy of the RDD partitioned using the specified partitioner. */
    @External def partitionBy(partitioner: Rep[SPartitioner]): Rep[SRDD[(K, V)]]

    /** Merges the values for each key using an associative reduce function. */
    @External def reduceByKey(func: Rep[((V, V)) => V]): Rep[SRDD[(K, V)]]

    /** Returns the list of values in the RDD for the key */
    @External def lookup(key: Rep[K]): Rep[SSeq[V]]

    /** Return an RDD containing all pairs of elements with matching keys */
    @External def join[W: Elem](other: Rep[SRDD[(K, W)]]): Rep[SRDD[(K, (V, W))]]
  }

  trait SPairRDDFunctionsCompanion {
    @Constructor def apply[K: Elem, V: Elem](rdd: Rep[SRDD[(K, V)]]): Rep[SPairRDDFunctions[K, V]]
  }

  implicit def rddToPairRddFunctions[K: Elem, V: Elem](rdd: Rep[SRDD[(K, V)]]): Rep[SPairRDDFunctions[K, V]] = {
    SPairRDDFunctions(rdd)
  }

  def DefaultOfPairRDDFunctions[K:Elem, V:Elem]: Default[PairRDDFunctions[K,V]] = {
    val pairs = sparkContext.parallelize(Seq.empty[(K,V)])
    Default.defaultVal(new PairRDDFunctions(pairs)(element[K].classTag, element[V].classTag))
  }
}

trait PairRDDFunctionssDsl extends impl.PairRDDFunctionssAbs  { self: SparkDsl => }
trait PairRDDFunctionssDslSeq extends impl.PairRDDFunctionssSeq { self: SparkDslSeq => }
trait PairRDDFunctionssDslExp extends impl.PairRDDFunctionssExp { self: SparkDslExp => }