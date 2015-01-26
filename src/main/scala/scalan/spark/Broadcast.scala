package scalan.spark

import scalan._
import org.apache.spark.broadcast.Broadcast

/** A broadcast variable. It allows to keep a read-only variable cached on each machine
  * rather than shipping a copy of it with tasks. */
trait Broadcasts extends Base with BaseTypes { self: BroadcastsDsl =>
  type RepBroadcast[A] = Rep[Broadcast[A]]

  trait SBroadcast[A] extends BaseTypeEx[Broadcast[A], SBroadcast[A]] { self =>
    implicit def eA: Elem[A]

    /** Gets the current value of the broadcast variable */
    @External def value: Rep[A]
  }
}

trait BroadcastsDsl extends impl.BroadcastsAbs
trait BroadcastsDslSeq extends impl.BroadcastsSeq
trait BroadcastsDslExp extends impl.BroadcastsExp
