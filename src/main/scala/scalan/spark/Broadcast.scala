package scalan.spark

import scalan._
//import scalan.common.Default
import org.apache.spark.broadcast.Broadcast

trait Broadcasts extends Base with BaseTypes { self: BroadcastsDsl =>
  type RepBroadcast[A] = Rep[Broadcast[A]]

  /** A broadcast variable. It allows to keep a read-only variable cached on each machine
    * rather than shipping a copy of it with tasks. */
  trait SBroadcast[A] extends BaseTypeEx[Broadcast[A], SBroadcast[A]] { self =>
    implicit def eA: Elem[A]

    /** Gets the current value of the broadcast variable */
    @External def value: Rep[A]
  }
/*
  trait SBroadcastCompanion extends ExCompanion1[Broadcast]  {
    @External def empty[A:Elem]: Rep[Broadcast[A]]
  }

  implicit def DefaultOfBroadcast[A:Elem]: Default[Broadcast[A]] = Default.defaultVal(Broadcast.empty[A])
  */
}

trait BroadcastsDsl extends impl.BroadcastsAbs
trait BroadcastsDslSeq extends impl.BroadcastsSeq
trait BroadcastsDslExp extends impl.BroadcastsExp
