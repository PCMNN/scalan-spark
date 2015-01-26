package scalan.spark

trait SparkDsl extends SparkContextDsl
with RDDsDsl
with PairRDDsDsl
with BroadcastsDsl
with PartitionerDsl

trait SparkDslSeq extends  SparkContextDslSeq
with RDDsDslSeq
with PairRDDsDslSeq
with BroadcastsDslSeq
with PartitionerDslSeq

trait SparkDslExp extends SparkContextDslExp
with RDDsDslExp
with PairRDDsDslExp
with BroadcastsDslExp
with PartitionerDslExp
