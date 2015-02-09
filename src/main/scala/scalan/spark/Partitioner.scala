package scalan.spark

import scalan._
import scalan.common.Default
import org.apache.spark.{HashPartitioner, Partitioner}

trait Partitioners extends Base with BaseTypes { self: SparkDsl =>
  type RepPartitioner = Rep[Partitioner]

  /** Partitioner defines how the elements in a key-value pair RDD are partitioned by key. */
  trait SPartitioner extends BaseTypeEx[Partitioner, SPartitioner]

  trait SPartitionerCompanion

  implicit def defaultPartitioner: Default[Partitioner] = {
    Default.defaultVal(new HashPartitioner(0))
  }
}

trait PartitionersDsl extends impl.PartitionersAbs  { self: SparkDsl => }
trait PartitionersDslSeq extends impl.PartitionersSeq { self: SparkDslSeq => }
trait PartitionersDslExp extends impl.PartitionersExp { self: SparkDslExp => }
