package scalan.spark

import scala.reflect.ClassTag
import scalan._

trait SparkDsl extends ScalanCommunityDsl
with SparkContextsDsl
with SparkConfsDsl
with RDDsDsl
with PairRDDFunctionssDsl
with PartitionersDsl
with BroadcastsDsl
{ implicit def elementToClassTag[A](implicit e: Elem[A]): ClassTag[A] = e.classTag }

trait SparkDslSeq extends SparkDsl with ScalanCommunityDslSeq
with SparkContextsDslSeq
with SparkConfsDslSeq
with RDDsDslSeq
with PairRDDFunctionssDslSeq
with PartitionersDslSeq
with BroadcastsDslSeq

trait SparkDslExp extends SparkDsl with ScalanCommunityDslExp
with SparkContextsDslExp
with SparkConfsDslExp
with RDDsDslExp
with PairRDDFunctionssDslExp
with PartitionersDslExp
with BroadcastsDslExp
