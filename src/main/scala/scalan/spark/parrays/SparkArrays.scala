package scalan.spark.parrays

import org.apache.spark.rdd.RDD
import scalan._
import scalan.common.OverloadHack.Overloaded1
import scalan.parrays._
import scalan.spark._
import scalan.common.Default
import org.apache.spark.SparkContext

trait SparkArrays extends PArrays { self: SparkDsl =>

  type SA[+A] = Rep[SparkArray[A]]
  trait SparkArray[A] extends PArray[A]
  trait SparkArrayCompanion extends PArrayCompanion

  abstract class RDDArray[A](val rdd: Rep[RDD[A]])(implicit val eA: Elem[A]) extends SparkArray[A] {
    def elem = eA
    def length = rdd.count.toInt
    def arr = rdd.collect
    def apply(i: Rep[Int]): Rep[A] = {
      rdd.zipWithIndex.filter(fun {(pair: Rep[(A, Long)]) =>
        IF (i === pair._2.toInt) THEN true ELSE false
      }).map(fun {(in: Rep[(A, Long)]) => in._1}).first
    }
    @OverloadId("many")
    def apply(indices: Arr[Int])(implicit o: Overloaded1): SA[A] = {
      val irdd = SRDD.fromArray(indices).map(fun {(i: Rep[Int]) => Pair(i.toLong, 0)})
      val vrdd = rdd.zipWithIndex
      val joinedRdd: RepRDD[(Long, (Int, A))] = irdd.join(vrdd)

      joinedRdd.map(fun {(in: Rep[(Long, (Int, A))]) =>
        val Pair(_, Pair(_, a: Rep[A])) = in
        a
      })
    }
    def mapBy[B: Elem](f: Rep[A => B]): SA[B] = rdd.map(f)
    def map[B: Elem](f: Rep[A] => Rep[B]): SA[B] = rdd.map(fun(f))
    def slice(offset: Rep[Int], length: Rep[Int]): Rep[PArray[A]] = {
      val indices = Array.rangeFrom0(length).map(_ + offset)
      apply(indices)
    }
    def reduce(implicit m: RepMonoid[A]): Rep[A] = {
      rdd.fold(m.zero)(fun {(in: Rep[(A, A)]) => m.append(in)})
    }
  }

  trait RDDArrayCompanion extends ConcreteClass1[RDDArray]
}

trait SparkArraysDsl extends impl.SparkArraysAbs
trait SparkArraysDslSeq extends impl.SparkArraysSeq
trait SparkArraysDslExp extends impl.SparkArraysExp

