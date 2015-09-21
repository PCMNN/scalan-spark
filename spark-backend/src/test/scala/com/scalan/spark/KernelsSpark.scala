/*
package com.scalan.spark

/**
 * Created by Victor Smirnov on 7/27/15.
 */

import la.SparkLADsl
import scala.language.reflectiveCalls
import scalan.common.OverloadHack.{Overloaded1, Overloaded2, Overloaded3, Overloaded4, Overloaded5}

trait KernelsSpark {

  trait Functions extends SparkLADsl {

    //concrete types for specializations
    type DV = DenseVector[Double]
    type SV = SparseVector[Double]
    type CV = SparseVector1[Double]
    type DM = SparkDenseIndexedMatrix[Double]
    type SM = SparkSparseIndexedMatrix[Double]

    def vvm[T: Elem: Numeric](vA: Vector[T], vB: Vector[T]) = vA dot vB
    //def mvm[T: Numeric](mA: Matrix[T], vB: Vector[T]) = mA * vB
    def mvm[T: Elem: Numeric](m: Matrix[T], v: Vector[T]) = DenseVector(m.rows.map(r => r dot v))
    //def mmm[T: Numeric](mA: Matrix[T], mB: Matrix[T]) = mA * mB
    def mmm[T: Elem: Numeric](mA: Matrix[T], mB: Matrix[T]) = mA.map(row => DenseVector(mB.columns.map(col => row dot col)))
    def conv[T: Elem: Numeric](m: Matrix[T], v: Vector[T]) = (m, v)
    def conv1[T: Elem: Numeric](m: Matrix[T]) = m
  }

  trait Specs_MVM extends Functions {

    def conv1SMDM(m: Rep[SM]): Rep[SparkDenseIndexedMatrix[Double]] = conv1(m).convertTo[DM]
    def convSMDMDV(m: Rep[SM], v: Rep[DV]): (Rep[DM], Rep[DV]) = (m.convertTo[DM], v.convertTo[DV])
    def convSMDMSV(m: Rep[SM], v: Rep[SV]): Rep[(DM, SV)] = Tuple(m.convertTo[DM], v.convertTo[SV])

    def wrap(x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], (Array[Int], (Array[Double], Int))))]) = {
      val Tuple(rddI, rddV, is, vs, n) = x
      val matrix: Rep[SparkSparseIndexedMatrix[Double]] = SparkSparseIndexedMatrix(RDDIndexedCollection(rddI), RDDIndexedCollection(rddV), n)
      val vector: Rep[SparseVector[Double]] = SparseVector(Collection(is), Collection(vs), n)
      (matrix, vector)
    }
    //def unwrap1(y: Rep[SparkDenseIndexedMatrix[Double]]) = y.rddValues.rdd
    def unwrap(in: Rep[(DM, SV)]) = {
      val Tuple(x, y) = in
      Tuple(x.rddValues.rdd, y.nonZeroIndices.arr, y.nonZeroValues.arr, y.length)
    }
  }

  trait SparkMVM extends Specs_MVM {

    //lazy val smdm_conv1 = fun { x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int))] => unwrap(conv1SMDM(wrap(x))) }
    lazy val smdmsv_conv = fun { x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], (Array[Int], (Array[Double], Int))))] =>
      val (m, v) = wrap(x)
      unwrap(convSMDMSV(m, v)) }
  }
}
*/
