package scalan.spark

import scalan._
import scalan.common.Default
import org.apache.spark.{HashPartitioner, Partitioner}

trait Partitioners extends Base with BaseTypes { self: SparkDsl =>
  type RepBasePartitioner = Rep[SBasePartitioner]

  trait SPartitioner extends Reifiable[SPartitioner] {
    def wrappedValueOfBaseType: Rep[Partitioner]
  }
  trait SPartitionerCompanion
  implicit def unwrapValueOfSPartitioner(w: Rep[SPartitioner]): Rep[Partitioner] = w.wrappedValueOfBaseType

  /** Partitioner defines how the elements in a key-value pair RDD are partitioned by key. */
  trait SBasePartitioner extends BaseTypeEx[Partitioner, SBasePartitioner] with SPartitioner { self =>
    def wrappedValueOfBaseType: Rep[Partitioner]
  }

  trait SBasePartitionerCompanion

  def DefaultOfPartitioner: Default[Partitioner] = {
    Default.defaultVal(new HashPartitioner(0))
  }
}

trait PartitionersDsl extends impl.PartitionersAbs  { self: SparkDsl => }
trait PartitionersDslSeq extends impl.PartitionersSeq { self: SparkDslSeq =>
  //implicit def toPartitioner(p: SPartitioner): Partitioner =
}
trait PartitionersDslExp extends impl.PartitionersExp { self: SparkDslExp => }
