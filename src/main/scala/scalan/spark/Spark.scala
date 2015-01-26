package scalan.spark

trait SparkDsl extends SparkContextDsl
with RDDsDsl
with PairRDDDsl
with BroadcastDsl
with PartitionerDsl

trait SparkDslSeq extends  SparkContextDslSeq
with RDDsDslSeq
with PairRDDDslSeq
with BroadcastDslSeq
with PartitionerDslSeq

trait SparkDslExp extends SparkContextDslExp
with RDDsDslExp
with PairRDDDslExp
with BroadcastDslExp
with PartitionerDslExp
