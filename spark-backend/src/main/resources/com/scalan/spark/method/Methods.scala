package com.scalan.spark.method

import org.apache.spark.rdd.{PairRDDFunctions, RDD}

import scala.reflect.ClassTag

object Methods {

  def reduceByKey[K : ClassTag, V : ClassTag](pRDDF: PairRDDFunctions[K, V], func: ((V, V)) => V) : PairRDDFunctions[K, V] =
    new PairRDDFunctions[K, V](pRDDF.reduceByKey((p1, p2) => func((p1, p2))))

  def fold[T](obj: RDD[T], zeroValue: T, op: ((T, T)) => T): T = obj.fold(zeroValue)((p1, p2) => op((p1, p2)))
}