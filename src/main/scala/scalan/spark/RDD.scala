package scalan.spark

import scala.reflect.ClassTag
import scalan._
import org.apache.spark.rdd.RDD
import scalan.common.Default

import scalan.common.Default

trait RDDs extends Base with BaseTypes { self: SparkDsl =>
  type RepRDD[A] = Rep[RDD[A]]

  /** The trait contains the basic operations available on all RDDs */
  trait SRDD[A] extends BaseTypeEx[RDD[A], SRDD[A]] { self =>
    implicit def eA: Elem[A]
    def wrappedValueOfBaseType: Rep[RDD[A]]

    /** Transformations **/

    /** Applies a function to all elements of this RDD end returns new RDD **/
    @External def map[B: Elem](f: Rep[A => B]): Rep[RDD[B]]

    /** Applies a function to all elements of the RDD and returns flattening the results */
    @External def flatMap[B: Elem](f: Rep[A => TraversableOnce[B]]): Rep[RDD[B]]

    /** Returns the union of this RDD and another one. */
    @External def union(other: Rep[RDD[A]]): Rep[RDD[A]]
    def ++(other: Rep[RDD[A]]): Rep[RDD[A]] = this.union(other)

    /** Aggregates the elements of each partition, and then the results for all the partitions */
    @External def fold(zeroValue: Rep[A])(op: Rep[((A, A)) => A]): Rep[A]

    /** Return an array with all elements of RDD */
    @External def collect(): Rep[Array[A]]

    /** Returns the RDD of all pairs of elements (a, b) where a is in `this` and b is in `other` */
    @External def cartesian[B: Elem](other: Rep[RDD[B]]): Rep[RDD[(A, B)]]

    /** Returns an RDD with the elements from `this` that are not in `other`. */
    @External def subtract(other: Rep[RDD[A]]): Rep[RDD[A]]

                                 /** Actions **/

    /** Returns the first element in this RDD. */
    @External def first(): Rep[A]
  }

  trait SRDDCompanion

  implicit def DefaultOfRDD[A:Elem]: Default[RDD[A]] = {
    val rdd = sparkContext.parallelize(Seq.empty[A])
    Default.defaultVal(rdd)
  }
}

trait RDDsDsl extends impl.RDDsAbs  { self: SparkDsl => }
trait RDDsDslSeq extends impl.RDDsSeq { self: SparkDslSeq => }
trait RDDsDslExp extends impl.RDDsExp { self: SparkDslExp => }
