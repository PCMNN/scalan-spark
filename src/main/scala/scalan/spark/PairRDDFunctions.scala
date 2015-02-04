package scalan.spark

import scala.reflect.ClassTag
import org.apache.spark.rdd._
import scalan._
import scalan.common.Default
import org.apache.spark.Partitioner

trait PairRDDFunctionss extends Base with BaseTypes { self: SparkDsl =>
  type RepPairRDDFunctions[K, V] = Rep[PairRDDFunctions[K, V]]

  /** Extra functions available on RDDs of (key, value) pairs */
  trait SPairRDDFunctions[K, V] extends BaseTypeEx[PairRDDFunctions[K, V], SPairRDDFunctions[K, V]] { self =>
    implicit def eK: Elem[K]
    implicit def eV: Elem[V]

    /** Returns a copy of the RDD partitioned using the specified partitioner. */
    @External def partitionBy(partitioner: Rep[Partitioner]): Rep[RDD[(K, V)]]

    /** Merges the values for each key using an associative reduce function. */
    @External def reduceByKey(func: Rep[((V, V)) => V]): Rep[PairRDDFunctions[K, V]]

    /** Returns an RDD with the values of each tuple. */
    @External def values: Rep[RDD[V]]
  }

  trait SPairRDDFunctionsCompanion {
    @Constructor def apply[K: Elem, V: Elem](rdd: Rep[RDD[(K, V)]]): Rep[PairRDDFunctions[K, V]]
  }

  implicit def rddToPairRddFunctions[K: Elem, V: Elem](rdd: Rep[RDD[(K, V)]]): Rep[PairRDDFunctions[K, V]] = {
    SPairRDDFunctions(rdd)
  }

  implicit def DefaultOfPairRDDFunctions[K:Elem, V:Elem]: Default[PairRDDFunctions[K,V]] = {
    val pairs = sparkContext.parallelize(Seq.empty[(K,V)])
    Default.defaultVal(new PairRDDFunctions(pairs)(element[K].classTag, element[V].classTag))
  }
}

trait PairRDDFunctionssDsl extends impl.PairRDDFunctionssAbs  { self: SparkDsl => }
/*
  implicit class Pair2PairRDD[K: Elem, V: Elem](that: Rep[PairRDDFunctions[K, V]]) {
    def reduceByKey(func: Rep[((V, V)) => V]): Rep[PairRDDFunctions[K, V]] = that.reduceByKey(func)
    def values: Rep[RDD[V]] = that.values
  }
*/

trait PairRDDFunctionssDslSeq extends impl.PairRDDFunctionssSeq { self: SparkDslSeq =>

  trait SeqSPairRDDFunctions[K,V] extends SPairRDDFunctionsImpl[K,V] {

    override def reduceByKey(func: Rep[((V, V)) => V]): Rep[PairRDDFunctions[K, V]] =
      new PairRDDFunctions(wrappedValueOfBaseType.reduceByKey((a1, a2) => func(a1, a2)))(eK.classTag, eV.classTag)
  }
}
trait PairRDDFunctionssDslExp extends impl.PairRDDFunctionssExp { self: SparkDslExp => }