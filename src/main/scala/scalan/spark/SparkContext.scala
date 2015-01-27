package scalan.spark

import scalan._
import scalan.common.Default
import org.apache.spark.SparkContext

trait SparkContexts extends Base with BaseTypes { self: SparkDsl =>

  type RepSparkContext = Rep[SparkContext]
  /** Current Spark Context */
  val sparkContext: SparkContext

  /** A SparkContext represents the connection to a Spark
    * cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster.*/
  trait SSparkContext extends BaseTypeEx[SparkContext, SSparkContext] {self =>
    /** Default level of parallelism */
    @External def defaultParallelism: Rep[Int]

    /** Creates a read-only variable in the cluster */
    @External def broadcast[T](value: Rep[T]): RepBroadcast[T]

    /** Creates a RDD based on a Scala collection */
    @External def makeRDD[T](seq: Rep[Seq[T]], numSlices: Rep[Int] = defaultParallelism): RepRDD[T]

    /** Creates an RDD without elements and partitions */
    @External def emptyRDD[T]: RepRDD[T]

    /** Shut down the SparkContext. */
    @External def stop
  }

  trait SSparkContextCompanion

  implicit def DefaultOfSparkContext: Default[SparkContext] = {
    Default.defaultVal(sparkContext)
  }
}

trait SparkContextsDsl extends impl.SparkContextsAbs { self: SparkDsl => }
trait SparkContextsDslSeq extends impl.SparkContextsSeq { self: SparkDslSeq => }
trait SparkContextsDslExp extends impl.SparkContextsExp { self: SparkDslExp => }

