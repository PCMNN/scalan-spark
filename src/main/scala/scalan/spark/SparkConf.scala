package scalan.spark

import scalan._
import scalan.common.Default
import org.apache.spark.SparkConf

trait SparkConfs extends Base with TypeWrappers { self: SparkDsl =>
  type RepSparkConf = Rep[SSparkConf]

  /** Configuration for a Spark application.
    * SparkConf allows to set various Spark parameters as key-value pairs.*/
  trait SSparkConf extends TypeWrapper[SparkConf, SSparkConf] {self =>
    def wrappedValueOfBaseType: Rep[SparkConf]

    /** Sets a name for the application. */
    @External def setAppName(name: Rep[String]): Rep[SSparkConf]

    /** URL of the master. For example, local, local[8] or spark://master:7077 */
    @External def setMaster(master: Rep[String]): Rep[SSparkConf]

    /** Set a configuration variable. */
    @External def set(key: Rep[String], value: Rep[String]): Rep[SSparkConf]
  }

  trait SSparkConfCompanion extends ExCompanion0[SSparkConf]  {
    @Constructor def apply(): Rep[SSparkConf]
  }

  def DefaultOfSparkConf: Default[SparkConf] = {
    null //Default.defaultVal(sparkContext.getConf)
  }
}

trait SparkConfsDsl extends impl.SparkConfsAbs  { self: SparkDsl => }
trait SparkConfsDslSeq extends impl.SparkConfsSeq { self: SparkDslSeq => }
trait SparkConfsDslExp extends impl.SparkConfsExp { self: SparkDslExp => }
