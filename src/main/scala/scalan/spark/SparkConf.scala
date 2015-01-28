package scalan.spark

import scalan._
import scalan.common.Default
import org.apache.spark.SparkConf

trait SparkConfs extends Base with BaseTypes { self: SparkDsl =>
  type RepSparkConf = Rep[SparkConf]

  /** Configuration for a Spark application.
    * SparkConf allows to set various Spark parameters as key-value pairs.*/
  trait SSparkConf extends BaseTypeEx[SparkConf, SSparkConf] {self =>
    /** Sets a name for the application. */
    @External def setAppName(name: Rep[String]): Rep[SparkConf]

    /** URL of the master. For example, local, local[8] or spark://master:7077 */
    @External def setMaster(master: Rep[String]): Rep[SparkConf]

    /** Set a configuration variable. */
    @External def set(key: Rep[String], value: Rep[String]): Rep[SparkConf]
  }

  trait SSparkConfCompanion extends ExCompanion0[SparkConf]  {
    @Constructor def apply(): Rep[SparkConf]
  }

  implicit def DefaultOfSparkConf: Default[SparkConf] = {
    Default.defaultVal(sparkContext.getConf)
  }
}

trait SparkConfsDsl extends impl.SparkConfsAbs  { self: SparkDsl => }
trait SparkConfsDslSeq extends impl.SparkConfsSeq { self: SparkDslSeq => }
trait SparkConfsDslExp extends impl.SparkConfsExp { self: SparkDslExp => }
