package scalan.spark

import scalan._
import org.apache.spark.rdd.RDD
import scalan.common.Default

trait RDDs extends Base with BaseTypes { self: SparkDsl =>
  type RepRDD[A] = Rep[RDD[A]]

  /** The trait contains the basic operations available on all RDDs */
  trait SRDD[A] extends BaseTypeEx[RDD[A], SRDD[A]] { self =>
    implicit def eA: Elem[A]
                                /** Transformations **/

    /** Applies a function to all elements of this RDD end returns new RDD **/
    @External def map[B: Elem](f: Rep[A => B]): Rep[RDD[B]]

    /** Gets a new RDD containing only the elements that satisfy a predicate. */
    @External def filter(f: Rep[A => Boolean]): Rep[RDD[A]]

    /** Applies a function to all elements of the RDD and returns flattening the results */
    @External def flatMap[B: Elem](f: Rep[A => TraversableOnce[B]]): Rep[RDD[B]]

    /** Returns the union of this RDD and another one. */
    @External def union(other: Rep[RDD[A]]): Rep[RDD[A]]
    def ++(other: Rep[RDD[A]]): Rep[RDD[A]] = this.union(other)

    /** Aggregates the elements of each partition, and then the results for all the partitions */
    @External def fold(zeroValue: Rep[A])(op: Rep[((A, A)) => A]): Rep[A]

    /** Returns the RDD of all pairs of elements (a, b) where a is in `this` and b is in `other` */
    @External def cartesian[B: Elem](other: Rep[RDD[B]]): Rep[RDD[(A, B)]]

    /** Returns an RDD with the elements from `this` that are not in `other`. */
    @External def subtract(other: Rep[RDD[A]]): Rep[RDD[A]]

    /** Zips this RDD with its element indices. */
    @External def zipWithIndex(): Rep[RDD[(A, Long)]]

    /** Persists RDD's values across operations after the first time it is computed.  */
    @External def cache: Rep[RDD[A]]

                                 /** Actions **/

    /** Returns the first element in this RDD. */
    @External def first: Rep[A]

    /** Returns the number of elements in the RDD. */
    @External def count: Rep[Long]

    /** Returns an array that contains all of the elements in this RDD. */
    @External def collect: Rep[Array[A]]
  }

  trait SRDDCompanion extends ExCompanion1[RDD] {
    def apply[A: Elem](arr: Rep[Array[A]]): Rep[RDD[A]] = fromArray(arr)
    def fromArray[A: Elem](arr: Rep[Array[A]]): Rep[RDD[A]] = {
      repSparkContext.makeRDD(SSeq(arr))
    }
  }

  def DefaultOfRDD[A:Elem]: Default[RDD[A]] = {
    val rdd = sparkContext.parallelize(Seq.empty[A])
    Default.defaultVal(rdd)
  }
}

trait RDDsDsl extends impl.RDDsAbs { self: SparkDsl => }
trait RDDsDslSeq extends impl.RDDsSeq { self: SparkDslSeq => }
trait RDDsDslExp extends impl.RDDsExp { self: SparkDslExp => }
