package scalan.spark

trait SparkDsl extends SparkContextDsl
with RDDsDsl
with PairRDDsDsl
with BroadcastsDsl
with PartitionersDsl

trait SparkDslSeq extends  SparkContextDslSeq
with RDDsDslSeq
with PairRDDsDslSeq
with BroadcastsDslSeq
with PartitionersDslSeq

trait SparkDslExp extends SparkContextDslExp
with RDDsDslExp
with PairRDDsDslExp
with BroadcastsDslExp
with PartitionersDslExp
