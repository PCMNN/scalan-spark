package scalan.spark

trait Partitioners {
  /** The trait defines how the elements in a key-value pair RDD are partitioned by key.
   * They maps each key to a partition ID, from 0 to `numPartitions - 1`. **/
  trait SPartitioner

  trait SPartitionerCompanion
}
