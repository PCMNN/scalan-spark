package scalan.spark

import scala.reflect.ClassTag
import scalan._
import scalan.common.Default
import org.apache.spark.broadcast.{Broadcast => SparkBroadcast}

trait Broadcasts extends Base with BaseTypes { self: SparkDsl =>
  type RepBroadcast[A] = Rep[SparkBroadcast[A]]

  /** A broadcast variable. It allows to keep a read-only variable cached on each machine
    * rather than shipping a copy of it with tasks. */
  trait SBroadcast[A] extends BaseTypeEx[SparkBroadcast[A], SBroadcast[A]] { self =>
    implicit def eA: Elem[A]
    def wrappedValueOfBaseType: Rep[SparkBroadcast[A]]

    /** Gets the current value of the broadcast variable */
    @External def value: Rep[A]
  }

  trait SBroadcastCompanion

  implicit def DefaultOfSparkBroadcast[A :Elem]: Default[SparkBroadcast[A]] = {
    val eA = element[A]
    implicit val ctA = eA.classTag
    val defaultA: A = ctA.newArray(1)(0)
    Default.defaultVal(sparkContext.broadcast(defaultA))
  }

}

trait BroadcastsDsl extends impl.BroadcastsAbs  { self: SparkDsl => }
trait BroadcastsDslSeq extends impl.BroadcastsSeq { self: SparkDslSeq => }
trait BroadcastsDslExp extends impl.BroadcastsExp { self: SparkDslExp => }
