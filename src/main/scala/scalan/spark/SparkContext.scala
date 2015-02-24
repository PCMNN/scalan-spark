package scalan.spark

import scala.collection.Seq
import org.apache.spark.rdd.RDD
import scalan._
import scalan.common.Default
import org.apache.spark.{SparkConf, SparkContext}

trait SparkContexts extends Base with BaseTypes { self: SparkDsl =>

  type RepSparkContext = Rep[SSparkContext]
  /** Current Spark Context */
  val sparkContext: SparkContext
  val sSparkContext: SSparkContext
  val repSparkContext: Rep[SSparkContext]

  /** A SparkContext represents the connection to a Spark
    * cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster.*/
  trait SSparkContext extends BaseTypeEx[SparkContext, SSparkContext] {self =>
    def wrappedValueOfBaseType: Rep[SparkContext]

    /** Default level of parallelism */
    @External def defaultParallelism: Rep[Int]

    /** Creates a read-only variable in the cluster */
    @External def broadcast[T:Elem](value: Rep[T]): Rep[SBroadcast[T]]

    /** Creates a RDD based on a Scala collection */
    @External def makeRDD[T:Elem](seq: Rep[SSeq[T]], numSlices: Rep[Int] = defaultParallelism): Rep[SRDD[T]]

    /** Creates an RDD without elements and partitions */
    @External def emptyRDD[T:Elem]: Rep[SRDD[T]]
  }

  trait SSparkContextCompanion extends ExCompanion0[SSparkContext] {
    @Constructor def apply(conf: Rep[SSparkConf]): Rep[SSparkContext]
  }

  def DefaultOfSparkContext: Default[SparkContext] = {
    Default.defaultVal(sparkContext)
  }
}

trait SparkContextsDsl extends impl.SparkContextsAbs { self: SparkDsl => }
trait SparkContextsDslSeq extends impl.SparkContextsSeq { self: SparkDslSeq => }
trait SparkContextsDslExp extends impl.SparkContextsExp { self: SparkDslExp => }

