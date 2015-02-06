package scalan.spark.parrays

import scalan.OverloadId
import scalan.common.OverloadHack.Overloaded1
import scalan.parrays._
import scalan.spark._
import scalan.common.Default

trait PSparkArrays extends PArrays { self: SparkDsl with PArraysDsl =>

  abstract class PRDDArray[A](val rdd: RepRDD[A])(implicit val eA: Elem[A]) extends PArray[A] {
    def elem = eA
    def length = rdd.count
    def arr = rdd.collect
    def apply(i: Rep[Int]): Rep[A] = {
      rdd.zipWithIndex.filter(fun {(pair: Rep[(A, Long)]) =>
        IF (i === pair._2) THEN true ELSE false
      }).map(fun {(in: Rep[(A, Long)]) => in._1}).first()
    }
    @OverloadId("many")
    def apply(indices: Arr[Int])(implicit o: Overloaded1): PA[A] = {
      val irdd = fromArray(indices).map(fun {(i: Rep[Int]) =>
        (i, Default.defaultOf[A])
      })
      val vrdd = rdd.zipWithIndex
      val joinedRdd: RepRDD[(Long, (A, Option[A]))] = irdd.leftOuterJoin(vrdd, ???)
      val result = joinedRdd.map(fun {(in: Rep[(Long, (A, Option[A]))]) =>
        val Pair(_, Pair(defaultA, a)) = in
        a
      })
      result
    }
    def mapBy[B: Elem](f: Rep[A => B]): PA[B] = rdd.map(f)
    def zip[B: Elem](ys: PA[B]): PA[(A, B)] = rdd.zip(ys)
    def slice(offset: Rep[Int], length: Rep[Int]): Rep[PArray[A]] = {
      val indices = Array.rangeFrom0(length).map(_ + offset)
      apply(indices)
    }

    def reduce(implicit m: RepMonoid[A]): Rep[A] = {
      rdd.fold(m.zero)(fun {(in: Rep[(A, A)]) => m.append(in)})
    }

    def scan(implicit m: RepMonoid[A]): Rep[(PArray[A], A)] = {

    }
  }
}
