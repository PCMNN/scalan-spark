package scalan.spark

trait SparkDsl extends SparkContextDsl
with RDDsDsl
with PairRDDDsl
with BroadcastsDsl
with PartitionerDsl

trait SparkDslSeq extends  SparkContextDslSeq
with RDDsDslSeq
with PairRDDDslSeq
with BroadcastsDslSeq
with PartitionerDslSeq

trait SparkDslExp extends SparkContextDslExp
with RDDsDslExp
with PairRDDDslExp
with BroadcastsDslExp
with PartitionerDslExp
