package scalan.spark

import scala.reflect.ClassTag
import scalan.{ScalanExp, ScalanSeq, Scalan}

trait SparkDsl extends Scalan
with SparkContextsDsl
with SparkConfsDsl
with RDDsDsl
with PairRDDFunctionssDsl
with PartitionersDsl
with BroadcastsDsl {
  implicit def elementToClassTag[A](implicit e: Elem[A]): ClassTag[A] = e.classTag

}

trait SparkDslSeq extends SparkDsl with ScalanSeq
with SparkContextsDslSeq
with SparkConfsDslSeq
with RDDsDslSeq
with PairRDDFunctionssDslSeq
with PartitionersDslSeq
with BroadcastsDslSeq

trait SparkDslExp extends SparkDsl with ScalanExp
with SparkContextsDslExp
with SparkConfsDslExp
with RDDsDslExp
with PairRDDFunctionssDslExp
with PartitionersDslExp
with BroadcastsDslExp
