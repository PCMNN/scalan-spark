package scalan.spark

trait SparkDsl extends SparkContextsDsl
with RDDsDsl
with PairRDDsDsl
with BroadcastsDsl

trait SparkDslSeq extends  SparkContextsDslSeq
with RDDsDslSeq
with PairRDDsDslSeq
with BroadcastsDslSeq

trait SparkDslExp extends SparkContextsDslExp
with RDDsDslExp
with PairRDDsDslExp
with BroadcastsDslExp
