package scalan.spark

import scalan.{ScalanExp, ScalanSeq, Scalan}

trait SparkDsl extends Scalan
with SparkContextsDsl
with SparkConfsDsl
with RDDsDsl
with PairRDDsDsl
with BroadcastsDsl

trait SparkDslSeq extends SparkDsl with ScalanSeq
with SparkContextsDslSeq
with SparkConfsDslSeq
with RDDsDslSeq
with PairRDDsDslSeq
with BroadcastsDslSeq

trait SparkDslExp extends SparkDsl with ScalanExp
with SparkContextsDslExp
with SparkConfsDslExp
with RDDsDslExp
with PairRDDsDslExp
with BroadcastsDslExp
