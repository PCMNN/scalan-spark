package scalan.spark

import org.apache.spark.broadcast.Broadcast

import scala.reflect.ClassTag
import scalan._
import org.apache.spark.rdd.PairRDDFunctions

import scalan.common.Default

trait PairRDDFunctionss extends Base with BaseTypes { self: SparkDsl =>
  type RepPairRDDFunctions[K, V] = Rep[PairRDDFunctions[K, V]]

  /** Extra functions available on RDDs of (key, value) pairs */
  trait SPairRDDFunctions[K, V] extends BaseTypeEx[PairRDDFunctions[K, V], SPairRDDFunctions[K, V]] { self =>
    implicit def eK: Elem[K]
    implicit def eV: Elem[V]

    /** Returns a copy of the RDD partitioned using the specified partitioner. */
    //@External def partitionBy(partitioner: SPartitioner): RepPairRDD[K, V]

    /** Merges the values for each key using an associative reduce function. */
    @External def reduceByKey(func: Rep[((V, V)) => V]): Rep[PairRDDFunctions[K, V]]
  }

  trait SPairRDDFunctionsCompanion

  implicit def DefaultOfPairRDDFunctions[K:Elem, V:Elem]: Default[PairRDDFunctions[K,V]] = {
    val pairs = sparkContext.parallelize(Seq.empty[(K,V)])
    Default.defaultVal(new PairRDDFunctions(pairs)(element[K].classTag, element[V].classTag))
  }
}

trait PairRDDFunctionssDsl extends impl.PairRDDFunctionssAbs  { self: SparkDsl => }
trait PairRDDFunctionssDslSeq extends impl.PairRDDFunctionssSeq { self: SparkDslSeq =>

  trait SeqSPairRDDFunctions[K,V] extends SPairRDDFunctionsImpl[K,V] {

    override def reduceByKey(func: Rep[((V, V)) => V]): Rep[PairRDDFunctions[K, V]] =
      new PairRDDFunctions(wrappedValueOfBaseType.reduceByKey((a1, a2) => func(a1, a2)))(eK.classTag, eV.classTag)
  }
}
trait PairRDDFunctionssDslExp extends impl.PairRDDFunctionssExp { self: SparkDslExp => }