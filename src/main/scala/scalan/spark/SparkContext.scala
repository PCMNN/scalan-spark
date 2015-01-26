package scalan.spark

import scalan._
import scalan.spark.{RDD, Broadcast}
import org.apache.spark.SparkContext

/** A SparkContext represents the connection to a Spark
  * cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster.*/
trait SparkContexts extends Base
    with BaseTypes with Broadcasts with RDDs { self: SparkContextsDsl =>

  type RepSparkContext = Rep[SparkContext]

  trait SSparkContext extends BaseTypeEx[SparkContext, SSparkContext] {self =>
    /** Default level of parallelism */
    def defaultParallelism: Rep[Int]

    /** Creates a read-only variable in the cluster */
    def broadcast[T](value: Rep[T]): RepBroadcast[T]

    /** Creates a RDD based on a Scala collection */
    def makeRDD[T](seq: Rep[Seq[T]], numSlices: Rep[Int] = defaultParallelism): RepRDD[T]

    /** Creates an RDD without elements and partitions */
    def emptyRDD[T]: RepRDD[T]
  }
}

trait SparkContextsDsl extends impl.SparkContextsAbs
trait SparkContextsDslSeq extends impl.SparkContextsSeq
trait SparkContextsDslExp extends impl.SparkContextsExp
