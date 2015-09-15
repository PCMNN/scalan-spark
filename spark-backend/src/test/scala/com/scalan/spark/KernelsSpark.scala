/*
package com.scalan.spark

/**
 * Created by Victor Smirnov on 7/27/15.
 */

import la.SparkLADsl
import scala.language.reflectiveCalls
import scalan.common.OverloadHack.{Overloaded3, Overloaded2, Overloaded1}

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
  }

  trait Specs_MVM extends Functions {

    def dddmvmDD(vs: (Rep[DM], Rep[DV])) = mvm(vs._1.convertTo[DM], vs._2.convertTo[DV])
    def dddmvmDS(vs: (Rep[DM], Rep[DV])) = mvm(vs._1.convertTo[DM], vs._2.convertTo[SV])
    def dddmvmDC(vs: (Rep[DM], Rep[DV])) = mvm(vs._1.convertTo[DM], vs._2.convertTo[CV])
    def dddmvmSD(vs: (Rep[DM], Rep[DV])) = mvm(vs._1.convertTo[SM], vs._2.convertTo[DV])
    def dddmvmSC(vs: (Rep[DM], Rep[DV])) = mvm(vs._1.convertTo[SM], vs._2.convertTo[CV])
    def dddmvmSS(vs: (Rep[DM], Rep[DV])) = mvm(vs._1.convertTo[SM], vs._2.convertTo[SV])

    def dsdmvmDD(vs: (Rep[DM], Rep[SV])) = mvm(vs._1.convertTo[DM], vs._2.convertTo[DV])
    def dsdmvmDS(vs: (Rep[DM], Rep[SV])) = mvm(vs._1.convertTo[DM], vs._2.convertTo[SV])
    def dsdmvmDC(vs: (Rep[DM], Rep[SV])) = mvm(vs._1.convertTo[DM], vs._2.convertTo[CV])
    def dsdmvmSD(vs: (Rep[DM], Rep[SV])) = mvm(vs._1.convertTo[SM], vs._2.convertTo[DV])
    def dsdmvmSC(vs: (Rep[DM], Rep[SV])) = mvm(vs._1.convertTo[SM], vs._2.convertTo[CV])
    def dsdmvmSS(vs: (Rep[DM], Rep[SV])) = mvm(vs._1.convertTo[SM], vs._2.convertTo[SV])

    def sddmvmDD(vs: (Rep[SM], Rep[DV])) = mvm(vs._1.convertTo[DM], vs._2.convertTo[DV])
    def sddmvmDS(vs: (Rep[SM], Rep[DV])) = mvm(vs._1.convertTo[DM], vs._2.convertTo[SV])
    def sddmvmDC(vs: (Rep[SM], Rep[DV])) = mvm(vs._1.convertTo[DM], vs._2.convertTo[CV])
    def sddmvmSD(vs: (Rep[SM], Rep[DV])) = mvm(vs._1.convertTo[SM], vs._2.convertTo[DV])
    def sddmvmSC(vs: (Rep[SM], Rep[DV])) = mvm(vs._1.convertTo[SM], vs._2.convertTo[CV])
    def sddmvmSS(vs: (Rep[SM], Rep[DV])) = mvm(vs._1.convertTo[SM], vs._2.convertTo[SV])

    def ssdmvmDD(vs: (Rep[SM], Rep[SV])) = mvm(vs._1.convertTo[DM], vs._2.convertTo[DV])
    def ssdmvmDS(vs: (Rep[SM], Rep[SV])) = mvm(vs._1.convertTo[DM], vs._2.convertTo[SV])
    def ssdmvmDC(vs: (Rep[SM], Rep[SV])) = mvm(vs._1.convertTo[DM], vs._2.convertTo[CV])
    def ssdmvmSD(vs: (Rep[SM], Rep[SV])) = mvm(vs._1.convertTo[SM], vs._2.convertTo[DV])
    def ssdmvmSC(vs: (Rep[SM], Rep[SV])) = mvm(vs._1.convertTo[SM], vs._2.convertTo[CV])
    def ssdmvmSS(vs: (Rep[SM], Rep[SV])) = mvm(vs._1.convertTo[SM], vs._2.convertTo[SV])

    def wrap(x: Rep[(SRDD[(Long, Array[Double])], Array[Double])])(implicit o: Overloaded1) = {
      val Tuple(rdd, b) = x
      val matrix: Rep[SparkDenseIndexedMatrix[Double]] = SparkDenseIndexedMatrix(RDDIndexedCollection(rdd), b.length)
      val vector: Rep[DenseVector[Double]] = DenseVector(Collection(b))
      (matrix, vector)
    }
    def wrap(x: Rep[(SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int)))])(implicit o: Overloaded2) = {
      val Tuple(rdd, bi, bv, n) = x
      val matrix: Rep[SparkDenseIndexedMatrix[Double]] = SparkDenseIndexedMatrix(RDDIndexedCollection(rdd), n)
      val vector: Rep[SparseVector[Double]] = SparseVector(Collection(bi), Collection(bv), n)
      (matrix, vector)
    }
    def wrap(x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Array[Double]))])(implicit o: Overloaded3) = {
      val Tuple(rddI, rddV, b) = x
      val matrix: Rep[SparkSparseIndexedMatrix[Double]] = SparkSparseIndexedMatrix(RDDIndexedCollection(rddI), RDDIndexedCollection(rddV), b.length)
      val vector: Rep[DenseVector[Double]] = DenseVector(Collection(b))
      (matrix, vector)
    }
    def wrap(x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int))))]) = {
      val Tuple(rddI, rddV, bi, bv, n) = x
      val matrix: Rep[SparkSparseIndexedMatrix[Double]] = SparkSparseIndexedMatrix(RDDIndexedCollection(rddI), RDDIndexedCollection(rddV), n)
      val vector: Rep[SparseVector[Double]] = SparseVector(Collection(bi), Collection(bv), n)
      (matrix, vector)
    }
    def unwrap(y: Rep[DenseVector[Double]]) = isoC.from(isoD.from(y).convertTo(isoC.eTo))
    lazy val isoD = getIsoByElem(element[DenseVector[Double]]).asInstanceOf[Iso[Collection[Double], DenseVector[Double]]]
    lazy val isoC = getIsoByElem(element[CollectionOverArray[Double]]).asInstanceOf[Iso[Array[Double], CollectionOverArray[Double]]]
  }

  trait Specs_MMM extends Functions {

    def dddmmmDD(vs: (Rep[DM], Rep[DM])) = mmm(vs._1.convertTo[DM], vs._2.convertTo[DM])
    def dddmmmDS(vs: (Rep[DM], Rep[DM])) = mmm(vs._1.convertTo[DM], vs._2.convertTo[SM])
    def dddmmmSD(vs: (Rep[DM], Rep[DM])) = mmm(vs._1.convertTo[SM], vs._2.convertTo[DM])
    def dddmmmSS(vs: (Rep[DM], Rep[DM])) = mmm(vs._1.convertTo[SM], vs._2.convertTo[SM])

    def dsdmmmDD(vs: (Rep[DM], Rep[SM])) = mmm(vs._1.convertTo[DM], vs._2.convertTo[DM])
    def dsdmmmDS(vs: (Rep[DM], Rep[SM])) = mmm(vs._1.convertTo[DM], vs._2.convertTo[SM])
    def dsdmmmSD(vs: (Rep[DM], Rep[SM])) = mmm(vs._1.convertTo[SM], vs._2.convertTo[DM])
    def dsdmmmSS(vs: (Rep[DM], Rep[SM])) = mmm(vs._1.convertTo[SM], vs._2.convertTo[SM])

    def sddmmmDD(vs: (Rep[SM], Rep[DM])) = mmm(vs._1.convertTo[DM], vs._2.convertTo[DM])
    def sddmmmDS(vs: (Rep[SM], Rep[DM])) = mmm(vs._1.convertTo[DM], vs._2.convertTo[SM])
    def sddmmmSD(vs: (Rep[SM], Rep[DM])) = mmm(vs._1.convertTo[SM], vs._2.convertTo[DM])
    def sddmmmSS(vs: (Rep[SM], Rep[DM])) = mmm(vs._1.convertTo[SM], vs._2.convertTo[SM])

    def ssdmmmDD(vs: (Rep[SM], Rep[SM])) = mmm(vs._1.convertTo[DM], vs._2.convertTo[DM])
    def ssdmmmDS(vs: (Rep[SM], Rep[SM])) = mmm(vs._1.convertTo[DM], vs._2.convertTo[SM])
    def ssdmmmSD(vs: (Rep[SM], Rep[SM])) = mmm(vs._1.convertTo[SM], vs._2.convertTo[DM])
    def ssdmmmSS(vs: (Rep[SM], Rep[SM])) = mmm(vs._1.convertTo[SM], vs._2.convertTo[SM])

    def wrap(x: Rep[(SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Double])], Int))])(implicit o: Overloaded1) = {
      val Tuple(rdd1, rdd2, n) = x
      val matrix1: Rep[SparkDenseIndexedMatrix[Double]] = SparkDenseIndexedMatrix(RDDIndexedCollection(rdd1), n)
      val matrix2: Rep[SparkDenseIndexedMatrix[Double]] = SparkDenseIndexedMatrix(RDDIndexedCollection(rdd2), n)
      (matrix1, matrix2)
    }
    def wrap(x: Rep[(SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int)))])(implicit o: Overloaded2) = {
      val Tuple(rdd1, rdd2I, rdd2V, n) = x
      val matrix1: Rep[SparkDenseIndexedMatrix[Double]] = SparkDenseIndexedMatrix(RDDIndexedCollection(rdd1), n)
      val matrix2: Rep[SparkSparseIndexedMatrix[Double]] = SparkSparseIndexedMatrix(RDDIndexedCollection(rdd2I), RDDIndexedCollection(rdd2V), n)
      (matrix1, matrix2)
    }
    def wrap(x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Double])], Int)))])(implicit o: Overloaded3) = {
      val Tuple(rdd1I, rdd1V, rdd2, n) = x
      val matrix1: Rep[SparkSparseIndexedMatrix[Double]] = SparkSparseIndexedMatrix(RDDIndexedCollection(rdd1I), RDDIndexedCollection(rdd1V), n)
      val matrix2: Rep[SparkDenseIndexedMatrix[Double]] = SparkDenseIndexedMatrix(RDDIndexedCollection(rdd2), n)
      (matrix1, matrix2)
    }
    def wrap(x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int))))]) = {
      val Tuple(rdd1I, rdd1V, rdd2I, rdd2V, n) = x
      val matrix1: Rep[SparkSparseIndexedMatrix[Double]] = SparkSparseIndexedMatrix(RDDIndexedCollection(rdd1I), RDDIndexedCollection(rdd1V), n)
      val matrix2: Rep[SparkSparseIndexedMatrix[Double]] = SparkSparseIndexedMatrix(RDDIndexedCollection(rdd2I), RDDIndexedCollection(rdd2V), n)
      (matrix1, matrix2)
    }
    def unwrap(y: Rep[DenseVector[Double]]) = isoC.from(isoD.from(y).convertTo(isoC.eTo))
    lazy val isoD = getIsoByElem(element[DenseVector[Double]]).asInstanceOf[Iso[Collection[Double], DenseVector[Double]]]
    lazy val isoC = getIsoByElem(element[CollectionOverArray[Double]]).asInstanceOf[Iso[Array[Double], CollectionOverArray[Double]]]
  }

  trait SparkMVM extends Specs_MVM {

    lazy val dddmvm_dd = fun { x: Rep[(SRDD[(Long, Array[Double])], Array[Double])] => unwrap(dddmvmDD(wrap(x))) }
    lazy val dddmvm_ds = fun { x: Rep[(SRDD[(Long, Array[Double])], Array[Double])] => unwrap(dddmvmDS(wrap(x))) }
    lazy val dddmvm_dc = fun { x: Rep[(SRDD[(Long, Array[Double])], Array[Double])] => unwrap(dddmvmDC(wrap(x))) }
    lazy val dddmvm_sd = fun { x: Rep[(SRDD[(Long, Array[Double])], Array[Double])] => unwrap(dddmvmSD(wrap(x))) }
    lazy val dddmvm_ss = fun { x: Rep[(SRDD[(Long, Array[Double])], Array[Double])] => unwrap(dddmvmSS(wrap(x))) }
    lazy val dddmvm_sc = fun { x: Rep[(SRDD[(Long, Array[Double])], Array[Double])] => unwrap(dddmvmSC(wrap(x))) }
    lazy val dsdmvm_dd = fun { x: Rep[(SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int)))] => unwrap(dsdmvmDD(wrap(x))) }
    lazy val dsdmvm_ds = fun { x: Rep[(SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int)))] => unwrap(dsdmvmDS(wrap(x))) }
    lazy val dsdmvm_dc = fun { x: Rep[(SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int)))] => unwrap(dsdmvmDC(wrap(x))) }
    lazy val dsdmvm_sd = fun { x: Rep[(SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int)))] => unwrap(dsdmvmSD(wrap(x))) }
    lazy val dsdmvm_ss = fun { x: Rep[(SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int)))] => unwrap(dsdmvmSS(wrap(x))) }
    lazy val dsdmvm_sc = fun { x: Rep[(SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int)))] => unwrap(dsdmvmSC(wrap(x))) }
    lazy val sddmvm_dd = fun { x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Array[Double]))] => unwrap(sddmvmDD(wrap(x))) }
    lazy val sddmvm_ds = fun { x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Array[Double]))] => unwrap(sddmvmDS(wrap(x))) }
    lazy val sddmvm_dc = fun { x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Array[Double]))] => unwrap(sddmvmDC(wrap(x))) }
    lazy val sddmvm_sd = fun { x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Array[Double]))] => unwrap(sddmvmSD(wrap(x))) }
    lazy val sddmvm_ss = fun { x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Array[Double]))] => unwrap(sddmvmSS(wrap(x))) }
    lazy val sddmvm_sc = fun { x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Array[Double]))] => unwrap(sddmvmSC(wrap(x))) }
    lazy val ssdmvm_dd = fun { x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int))))] => unwrap(ssdmvmDD(wrap(x))) }
    lazy val ssdmvm_ds = fun { x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int))))] => unwrap(ssdmvmDS(wrap(x))) }
    lazy val ssdmvm_dc = fun { x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int))))] => unwrap(ssdmvmDC(wrap(x))) }
    lazy val ssdmvm_sd = fun { x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int))))] => unwrap(ssdmvmSD(wrap(x))) }
    lazy val ssdmvm_ss = fun { x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int))))] => unwrap(ssdmvmSS(wrap(x))) }
    lazy val ssdmvm_sc = fun { x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int))))] => unwrap(ssdmvmSC(wrap(x))) }
  }

  trait SparkMMM extends Specs_MMM {

    lazy val dddmmm_dd = fun { x: Rep[(SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Double])], Int))] => unwrap(dddmmmDD(wrap(x))) }
    lazy val dddmmm_ds = fun { x: Rep[(SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Double])], Int))] => unwrap(dddmmmDS(wrap(x))) }
    lazy val dddmmm_dc = fun { x: Rep[(SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Double])], Int))] => unwrap(dddmmmDC(wrap(x))) }
    lazy val dddmmm_sd = fun { x: Rep[(SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Double])], Int))] => unwrap(dddmmmSD(wrap(x))) }
    lazy val dddmmm_ss = fun { x: Rep[(SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Double])], Int))] => unwrap(dddmmmSS(wrap(x))) }
    lazy val dddmmm_sc = fun { x: Rep[(SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Double])], Int))] => unwrap(dddmmmSC(wrap(x))) }
    lazy val dsdmmm_dd = fun { x: Rep[(SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int)))] => unwrap(dsdmmmDD(wrap(x))) }
    lazy val dsdmmm_ds = fun { x: Rep[(SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int)))] => unwrap(dsdmmmDS(wrap(x))) }
    lazy val dsdmmm_dc = fun { x: Rep[(SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int)))] => unwrap(dsdmmmDC(wrap(x))) }
    lazy val dsdmmm_sd = fun { x: Rep[(SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int)))] => unwrap(dsdmmmSD(wrap(x))) }
    lazy val dsdmmm_ss = fun { x: Rep[(SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int)))] => unwrap(dsdmmmSS(wrap(x))) }
    lazy val dsdmmm_sc = fun { x: Rep[(SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int)))] => unwrap(dsdmmmSC(wrap(x))) }
    lazy val sddmmm_dd = fun { x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Double])], Int)))] => unwrap(sddmmmDD(wrap(x))) }
    lazy val sddmmm_ds = fun { x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Double])], Int)))] => unwrap(sddmmmDS(wrap(x))) }
    lazy val sddmmm_dc = fun { x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Double])], Int)))] => unwrap(sddmmmDC(wrap(x))) }
    lazy val sddmmm_sd = fun { x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Double])], Int)))] => unwrap(sddmmmSD(wrap(x))) }
    lazy val sddmmm_ss = fun { x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Double])], Int)))] => unwrap(sddmmmSS(wrap(x))) }
    lazy val sddmmm_sc = fun { x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Double])], Int)))] => unwrap(sddmmmSC(wrap(x))) }
    lazy val ssdmmm_dd = fun { x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int))))] => unwrap(ssdmmmDD(wrap(x))) }
    lazy val ssdmmm_ds = fun { x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int))))] => unwrap(ssdmmmDS(wrap(x))) }
    lazy val ssdmmm_dc = fun { x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int))))] => unwrap(ssdmmmDC(wrap(x))) }
    lazy val ssdmmm_sd = fun { x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int))))] => unwrap(ssdmmmSD(wrap(x))) }
    lazy val ssdmmm_ss = fun { x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int))))] => unwrap(ssdmmmSS(wrap(x))) }
    lazy val ssdmmm_sc = fun { x: Rep[(SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], (SRDD[(Long, Array[Int])], (SRDD[(Long, Array[Double])], Int))))] => unwrap(ssdmmmSC(wrap(x))) }
  }
}
*/
