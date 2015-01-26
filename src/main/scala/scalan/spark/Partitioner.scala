package scalan.spark

import scalan._
import org.apache.spark.Partitioner

trait Partitioners {
  type RepPartitioner = Rep[Partitioner]

  /** The trait defines how the elements in a key-value pair RDD are partitioned by key.
   * They maps each key to a partition ID, from 0 to `numPartitions - 1`. **/
  trait SPartitioner extends BaseTypeEx[Partitioner, SPartitioner] { self =>
    /** Total number of partitions */
    @External def numPartitions: Rep[Int]

    /** Gets partition number where the element has to be moved. */
    @External def getPartition(key: Rep[Any]): Rep[Int]
  }
}

trait PartitionersDsl extends impl.PartitionersAbs
trait PartitionersDslSeq extends impl.PartitionersSeq
trait PartitionersDslExp extends impl.PartitionersExp
