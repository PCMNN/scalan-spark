package scalan.spark

trait SparkDsl extends SparkContextsDsl
with SparkConfsDsl
with RDDsDsl
with PairRDDsDsl
with BroadcastsDsl

trait SparkDslSeq extends  SparkContextsDslSeq
with SparkConfsDslSeq
with RDDsDslSeq
with PairRDDsDslSeq
with BroadcastsDslSeq

trait SparkDslExp extends SparkContextsDslExp
with SparkConfsDslExp
with RDDsDslExp
with PairRDDsDslExp
with BroadcastsDslExp
