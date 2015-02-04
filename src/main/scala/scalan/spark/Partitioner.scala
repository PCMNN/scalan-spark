package scalan.spark

import scalan._
import scalan.common.Default
import org.apache.spark.{HashPartitioner, Partitioner}

trait Partitioners extends Base with BaseTypes { self: SparkDsl =>

  implicit def defaultPartitioner: Default[Partitioner] = {
    Default.defaultVal(new HashPartitioner(0))
  }

  implicit def partitionerElement: Element[Partitioner] = new BaseElem[Partitioner]
}
