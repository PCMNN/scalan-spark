package scalan.spark

import org.apache.spark.rdd.RDD

import scalan._
import scalan.common.Default
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.{Broadcast => SparkBroadcast}

trait SparkContexts extends Base with BaseTypes { self: SparkDsl =>

  type RepSparkContext = Rep[SparkContext]
  /** Current Spark Context */
  val sparkContext: SparkContext
  val repSparkContext: Rep[SparkContext]

  /** A SparkContext represents the connection to a Spark
    * cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster.*/
  trait SSparkContext extends BaseTypeEx[SparkContext, SSparkContext] {self =>
    /** Default level of parallelism */
    @External def defaultParallelism: Rep[Int]

    /** Creates a read-only variable in the cluster */
    @External def broadcast[T:Elem](value: Rep[T]): Rep[SparkBroadcast[T]]

    /** Creates a RDD based on a Scala collection */
    @External def makeRDD[T:Elem](seq: Rep[Seq[T]], numSlices: Rep[Int] = defaultParallelism): Rep[RDD[T]]

    /** Creates an RDD without elements and partitions */
    @External def emptyRDD[T:Elem]: Rep[RDD[T]]
  }

  trait SSparkContextCompanion extends ExCompanion0[SparkContext]  {
    @Constructor def apply(conf: Rep[SparkConf]): Rep[SparkContext]
  }

  implicit def DefaultOfSparkContext: Default[SparkContext] = {
    Default.defaultVal(sparkContext)
  }
}

trait SparkContextsDsl extends impl.SparkContextsAbs { self: SparkDsl => }
trait SparkContextsDslSeq extends impl.SparkContextsSeq { self: SparkDslSeq => }
trait SparkContextsDslExp extends impl.SparkContextsExp { self: SparkDslExp => }

