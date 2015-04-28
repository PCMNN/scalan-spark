package scalan.spark

import scalan._
import scalan.common.Default
import org.apache.spark.broadcast.Broadcast

trait Broadcasts extends Base with TypeWrappers { self: SparkDsl =>
  type RepBroadcast[A] = Rep[SBroadcast[A]]

  /** A broadcast variable. It allows to keep a read-only variable cached on each machine
    * rather than shipping a copy of it with tasks. */
  @ContainerType
  trait SBroadcast[A] extends TypeWrapper[Broadcast[A], SBroadcast[A]] { self =>
    implicit def eA: Elem[A]
    def wrappedValueOfBaseType: Rep[Broadcast[A]]
    def map[B: Elem](f: Rep[A => B]): RepBroadcast[B] = repSparkContext.broadcast(f(value))

    /** Gets the current value of the broadcast variable */
    @External def value: Rep[A]
  }

  trait SBroadcastCompanion {
    def empty[A: Elem]: Rep[SBroadcast[A]] = {
      val eA = element[A]
      repSparkContext.broadcast(eA.defaultRepValue)
    }
  }

  def DefaultOfBroadcast[A :Elem]: Default[Broadcast[A]] = {
    val eA = element[A]
    implicit val ctA = eA.classTag
    val defaultA: A = ctA.newArray(1)(0)
    Default.defaultVal(sparkContext.broadcast(defaultA))
  }
}

trait BroadcastsDsl extends impl.BroadcastsAbs  { self: SparkDsl => }
trait BroadcastsDslSeq extends impl.BroadcastsSeq { self: SparkDslSeq =>
  implicit def broadcastToSBroadcast[A:Elem](b: Broadcast[A]): SBroadcast[A] = SBroadcastImpl(b)

}
trait BroadcastsDslExp extends impl.BroadcastsExp { self: SparkDslExp => }
