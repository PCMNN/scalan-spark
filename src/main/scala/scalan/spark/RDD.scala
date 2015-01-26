package scalan.spark

import scalan._
import org.apache.spark.rdd.RDD

trait RDDs extends Base with BaseTypes { self: RDDsDsl =>
  type RepRDD[A] = Rep[RDD[A]]

  /** The trait contains the basic operations available on all RDDs */
  trait SRDD[A] extends BaseTypeEx[RDD[A], SRDD[A]] { self =>
    implicit def eA: Elem[A]

    /** Applies a function to all elements of this RDD end returns new RDD **/
    @External def map[B: Elem](f: Rep[A => B]): RepRDD[B]

    /** Applies a function to all elements of the RDD and returns flattening the results */
    @External def flatMap[B: Elem](f: Rep[A => TraversableOnce[B]]): RepRDD[B]

    /** Returns the union of this RDD and another one. */
    @External def union(other: RepRDD[A]): RepRDD[A]
    @External def ++(other: RepRDD[A]): RepRDD[A] = this.union(other)

    /** Aggregates the elements of each partition, and then the results for all the partitions */
    @External def fold(zeroValue: Rep[A])(op: Rep[(A, A) => A]): Rep[A]

    /** Returns the RDD of all pairs of elements (a, b) where a is in `this` and b is in `other` */
    @External def cartesian[B: Elem](other: RepRDD[B]): RepRDD[(A, B)]

    /** Returns an RDD with the elements from `this` that are not in `other`. */
    @External def subtract(other: RepRDD[A]): RepRDD[A]
  }
}

trait RDDsDsl extends impl.RDDsAbs
trait RDDsDslSeq extends impl.RDDsSeq
trait RDDsDslExp extends impl.RDDsExp
