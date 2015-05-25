package com.scalan.spark

/**
 * Created by afilippov on 5/19/15.
 */

import java.io.File

import com.scalan.spark.backend.SparkScalanCompiler
import la.{SparkLADslExp, SparkLADsl}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.scalatest.BeforeAndAfterAll

import scala.language.reflectiveCalls
import scalan.common.OverloadHack.Overloaded1
import scalan.it.ItTestsUtil
import scalan.ml.{CF, CFDslExp, ExampleBL}
import scalan.spark.SparkDsl
import scalan.{BaseTests, ScalanDsl}

class SVDppSparkTests extends BaseTests with BeforeAndAfterAll with ItTestsUtil { suite =>
  val pref = new File("test-out/scalan/spark/backend/")
  val globalSparkConf = null //new SparkConf().setAppName("R/W Broadcast").setMaster("local")
  var globalSparkContext: SparkContext = null

  override def beforeAll() = {
    //globalSparkContext = new SparkContext(globalSparkConf)
  }

  override def afterAll() = {
    //globalSparkContext.stop()
  }

  trait SVDppSpark extends SparkLADsl with CF {
    val prefix = suite.pref
    val subfolder = "simple"

    type SparkClosure = Rep[(ParametersPaired, (SparkSparseMatrix[Double], SparkSparseMatrix[Double]))]

    type SparkSVDModelVal = (DenseVector[Double], (DenseVector[Double],
      (SparkDenseMatrix[Double], (SparkDenseMatrix[Double], SparkDenseMatrix[Double]))))

    type SparkSVDModel = Rep[SparkSVDModelVal]

    type SparkSVDState = Rep[(SparkSVDModelVal, (Int, (Double, (Int, Double))))]

    type SparkModSVD = Rep[(SparkDenseMatrix[Double], SparkDenseMatrix[Double])]

    type SparkModSVDpp = Rep[(SparkDenseMatrix[Double], (SparkDenseMatrix[Double], SparkDenseMatrix[Double]))]


    def replicate[T: Elem](len: IntRep, v: Rep[T]): Coll[T] = Collection.replicate[T](len, v)
    def ReplicatedVector(len: IntRep, v: DoubleRep): Rep[DenseVector[Double]] = DenseVector(replicate(len, v))
    def zeroVector(len: IntRep): Rep[DenseVector[Double]] = DenseVector(Collection.replicate(len, 0.0))

    def RandomMatrix(sc: Rep[SSparkContext], numRows: IntRep, numColumns: IntRep, mean: DoubleRep, stddev: DoubleRep): Rep[SparkDenseMatrix[Double]] = {
      //val vals = SArray.replicate(numRows, SArray.replicate(numColumns, 0.0))
      val rddVals = sc.makeRDD(SSeq(SArray.replicate(numRows, 0))).map { i:Rep[Int] => SArray.replicate(numColumns, 0.0) }
      SparkDenseMatrix(RDDCollection(rddVals), numColumns)
    }

    def RatingsMatrix(sc: Rep[SSparkContext], rows: Rep[RDDCollection[SparseVector[Double]]], numColumns: IntRep): Rep[SparkSparseMatrix[Double]] = {
      val rddIndexes = rows.map{ vec => vec.nonZeroIndices.arr}.asRep[RDDCollection[Array[Int]]]
      val rddValues = rows.map{ vec => vec.nonZeroValues.arr}.asRep[RDDCollection[Array[Double]]]
      SparkSparseMatrix(rddIndexes, rddValues, numColumns)
    }

    def FactorsMatrix(sc: Rep[SSparkContext], rows: Coll[AbstractVector[Double]], numColumns: IntRep): Rep[SparkDenseMatrix[Double]] = {
      val rddValues = RDDCollection(sc.makeRDD(rows.map { vec => vec.items.arr}.seq)) //asRep[RDDCollection[Array[Double]]]
      SparkDenseMatrix(rddValues, numColumns)
    }

    def FactorsMatrixNew(sc: Rep[SSparkContext], rows: Rep[SRDD[Array[Double]]], numColumns: IntRep): Rep[SparkDenseMatrix[Double]] = {
      SparkDenseMatrix(RDDCollection(rows), numColumns)
    }

    def average(matrix: Rep[SparkSparseMatrix[Double]]) = {
      val coll = matrix.rows.flatMap(v => v.nonZeroValues)
      coll.reduce / coll.length.toDouble
    }

    def initSpark(sc: Rep[SSparkContext], nUsers: IntRep, nItems: IntRep, width: IntRep, stddev: DoubleRep) = {
      val vBu0 = zeroVector(nUsers)
      val vBi0 = zeroVector(nItems)
      val mP0 = RandomMatrix(sc,nUsers, width, zero, stddev)
      val mQ0 = RandomMatrix(sc,nItems, width, zero, stddev)
      val mY0 = RandomMatrix(sc,nItems, width, zero, stddev)
      Tuple(vBu0, vBi0, mP0, mQ0, mY0)
    }

    def stepSpark(closure: SparkClosure, cs0: Coll[Int], cs0T: Vector[Int], mu: DoubleRep)(state: SparkSVDState): SparkSVDState = {
      val Tuple(parameters, mR, mN) = closure
      val Tuple(maxIterations, convergeLimit, gamma1, gamma2, lambda6, lambda7, _, coeffDecrease) = parameters
      val Pair(model0, meta) = state
      val Tuple(_, _, _, stepDecrease) = meta
      val Tuple(vBu0, vBi0, mP0, mQ0, mY0) = model0
      val bl = Tuple(vBu0, vBi0)
      val svd = Tuple(mP0, mQ0)
      val svdpp = Tuple(mP0, mQ0, mY0)
      val mE = errorMatrixSpark(mR, mN, mu)(model0)
      val mEt = mE.transpose.asRep[SparkSparseMatrix[Double]] // FIXME: remove asRep
      val Tuple(vBu, vBi) = calculateBaselineSpark(mE, cs0, cs0T, mu, gamma1 * stepDecrease, lambda6)(bl)
      val mP = calculateMPSpark(mE, gamma2 * stepDecrease, lambda7)(svd)
      val mQ = calculateMQSpark(mE, mEt, mN, gamma2 * stepDecrease, lambda7)(svdpp)
      val mY = calculateMYSpark(mE, mEt, mN, gamma2 * stepDecrease, lambda7)(svdpp)
      //println("[CF] mP0: " + mP0)
      //println("[CF] mQ0: " + mQ0)
      //println("[CF] mY0: " + mY0)
      val rmse = calculateRMSESpark(mE)
      //println("[SVD++] rmse: " + rmse)
      val modelNew = Tuple(vBu, vBi, mP, mQ, mY)
      val stateNew = selectStateSpark(rmse, maxIterations, convergeLimit, coeffDecrease)(state, modelNew)
      stateNew
    }

    def convergedSpark(state: SparkSVDState): BoolRep = {
      val Pair(_, meta) = state
      val Tuple(_, _, exitFlag, _) = meta
      exitFlag !== flagRunning
    }

    def calculateRMSESpark(mE: Rep[SparkSparseMatrix[Double]]): DoubleRep = {
      val coll = mE.rows.flatMap(v => v.nonZeroValues.map(v => v * v))
      // TODO LMS: this code fails in LMS backend (the previous line passes)
      //val coll = mE.rows.flatMap(v => v.nonZeroValues).map(v => v * v)
      Math.sqrt(coll.reduce / coll.length.toDouble)
    }
    def calculateBaselineSpark(mE: Rep[SparkSparseMatrix[Double]], cs0: Coll[Int], cs0T: Vector[Int], mu: Rep[Double],
                          gamma1: Rep[Double], lambda6: Rep[Double])(baseline: Rep[(DenseVector[Double],DenseVector[Double])]) : Rep[(DenseVector[Double],DenseVector[Double])] = {
      val Pair(vBu0, vBi0) = baseline
      val c0 = one - gamma1 * lambda6
      val cs = cs0.map(n => power(c0, n))
      val csT = cs0T.items.map(n => power(c0, n))
      val vBu = (mE.reduceByRows *^ gamma1) +^ (vBu0 *^ cs)
      val vBi = (mE.reduceByColumns *^ gamma1) +^ (vBi0 *^ csT)
      (vBu.convertTo[DenseVector[Double]], vBi.convertTo[DenseVector[Double]])
    }

    // move to States DSL
    def selectStateSpark(rmseNew: DoubleRep, maxIterations: IntRep,
                    convergeLimit: DoubleRep, coeffDecrease: DoubleRep)
                   (state: SparkSVDState, modelNew: SparkSVDModel): SparkSVDState = {
      val Pair(model, meta) = state
      val Tuple(iteration, rmse, flagRunning, stepDecrease0) = meta
      val stepDecrease = coeffDecrease * stepDecrease0
      val res = IF (iteration >= maxIterations) THEN {
        //printString("Reached maximum number of iterations")
        Pair(modelNew, Tuple(iteration, rmseNew, flagOverMaxIter, stepDecrease))
      } ELSE {
        IF (rmseNew > rmse) THEN {
          //printString("Failed to converge any better")
          //printString(state.costValue.toString + " > " + rmse)
          Pair(model, Tuple(iteration, rmse, flagFailure1, stepDecrease))
        } ELSE {
          IF (rmse - rmseNew < convergeLimit) THEN {
            //printString("Converged to given limit")
            Pair(modelNew, Tuple(iteration, rmseNew, flagConverged, stepDecrease))
          } ELSE {
            //printString("Keep training")
            Pair(modelNew, Tuple(iteration + oneInt, rmseNew, flagRunning, stepDecrease))
          }
        }
      }
      //println(res._3)
      res
    }

    def applyLifted[ValueType: Elem](rdd: Rep[SRDD[ValueType]], irdd: Rep[SRDD[Array[Int]]]): Rep[SRDD[Array[ValueType]]] = {
      val rddWithIdxs: Rep[SPairRDDFunctions[Long, ValueType]] = rdd.zipWithIndex.map { fun{ in: Rep[(ValueType, Long)] => (in._2, in._1)} }

      val idxsWithIdxs: Rep[SRDD[(Array[Int], Long)]] = irdd.zipWithIndex
      val flatWithIdxs: Rep[SPairRDDFunctions[Long, Long]] = idxsWithIdxs.flatMap (fun { in: Rep[(Array[Int], Long)] => SSeq(in._1.map({ v: Rep[Int] => (v.toLong, in._2)})) })

      val groupped: Rep[SRDD[(Long, SSeq[Long])]] = SPairRDDFunctionsImpl(flatWithIdxs).groupByKey

      val joined: Rep[SRDD[(Long, (ValueType, SSeq[Long]))]] = SPairRDDFunctionsImpl(rddWithIdxs).join(groupped)

      val resFlat: Rep[SPairRDDFunctions[Long,ValueType]] = joined.flatMap (fun{ in: Rep[(Long, (ValueType, SSeq[Long]))] =>
        val seq: Rep[SSeq[Long]] = in._3
        val value: Rep[ValueType] = in._2
        seq.map( fun{ i: Rep[Long] => (i,value)} )
      })

      SPairRDDFunctionsImpl(resFlat).groupByKey.map (fun { in: Rep[(Long, SSeq[ValueType])] => in._2.toArray})
    }

    // move to Datasets DSL? Or better to Models DSL?
    def errorMatrixSpark(mR: Rep[SparkSparseMatrix[Double]], mN: Rep[SparkSparseMatrix[Double]], mu: DoubleRep)(model: SparkSVDModel): Rep[SparkSparseMatrix[Double]] = {
      //println("[SVD++] mu: " + mu)
      val Tuple(vBu, vBi, mP, mQ, mY) = model
      val nItems = mR.numColumns
      /* Initial vesrion:

      val vsE = (mR.rows zip (mP.rows zip (mN.rows zip vBu.items))).map { case Tuple(vR, vP, vN, bu) =>
        val indices = vN.nonZeroIndices
        val k = IF (indices.length > zeroInt) THEN { one / Math.sqrt(indices.length.toDouble) } ELSE zero
        val vY = mY(indices).reduceByColumns *^ k
        val vBii = vBi.items(vR.nonZeroIndices)
        val vPvY = vP +^ vY
        val mQReduced = mQ.rows(vR.nonZeroIndices) map { row => row dot vPvY }
        //println("[SVD++] vBii: " + vBii.arr.toList)
        //println("mQReduced: " + mQReduced.arr.toList)
        val newValues = (vR.nonZeroValues zip (vBii zip mQReduced)).map { case Tuple(r, bi, pq) =>
          r - mu - bu - bi - pq
        }
        //println("[SVD++] newValues: " + newValues.arr.toList)
        SparseVector(vR.nonZeroIndices, newValues, nItems)
      }

        TODO:
        There should be lifted version of each operation here.
        Currently, it is made manually
      */

      val indicesLifted: Rep[SRDD[Array[Int]]] = mN.rddIdxs.rdd
      val vrIndicesLifted: Rep[SRDD[Array[Int]]] = mR.rddIdxs.rdd

      val kLifted: Rep[SRDD[Double]] = indicesLifted.map { indices: Rep[Array[Int]] =>
        IF (indices.length > zeroInt) THEN { one / Math.sqrt(indices.length.toDouble) } ELSE zero
      }

      val arrayReduceFun =  fun {in: Rep[(Array[Double],Array[Double])] =>
        (in._1 zip in._2).map{ pair => pair._1 + pair._2}
      }

      val applyLiftedReduced: Rep[SRDD[Array[Double]]] =  applyLifted[Array[Double]](mY.rddVals.rdd, indicesLifted). map { in: Rep[Array[Array[Double]]] =>
        val numRows = in.length
        array_rangeFrom0(mY.numColumns).map { col: Rep[Int] =>
          array_rangeFrom0(numRows).map { row: Rep[Int] => in(row)(col) }.reduce
        }
      }

      val vPvYLifted: Rep[SRDD[Array[Double]]] = (applyLiftedReduced zip (mP.rddVals.rdd zip kLifted)) map ( fun { case Tuple(vY, vP, k) =>
        (vY zip vP).map { case Pair(y,p) => y * k + p}
      })

      val mQCutLifted: Rep[SRDD[Array[Array[Double]]]] = applyLifted[Array[Double]](mQ.rddVals.rdd, vrIndicesLifted)

      val mQLiftedReduced: Rep[SRDD[Array[Double]]] = (mQCutLifted zip vPvYLifted).map (fun { case Pair(mQCut, vPvY) =>
          mQCut.map { row: Rep[Array[Double]] =>
            (row zip vPvY).map { in: Rep[(Double, Double)] => in._1 * in._2}.reduce
          }
      })

      val vBrdd = mR.sc.makeRDD(vBu.items.seq)  // Lift non-distrubuted value, from closure
      val vsE = (mR.rddIdxs.rdd zip (mR.rddVals.rdd zip (vBrdd zip mQLiftedReduced))) map fun({ case Tuple(vRidxs, vRvals, bu, mQReduced) =>
        val vBii = vBi.items(Collection(vRidxs)).arr
        val newValues = (vRvals zip (vBii zip mQReduced)).map { case Tuple(r, bi, pq) =>
          r - mu - bu - bi - pq
        }
        SparseVector(Collection(vRidxs), Collection(newValues), nItems)
      })
      RatingsMatrix(mR.sc, RDDCollection(vsE), nItems)
    }

    // move to Models DSL
    def calculateMPSpark(mE: Rep[SparkSparseMatrix[Double]], gamma2: DoubleRep, lambda7: DoubleRep)(svd: SparkModSVD): Rep[SparkDenseMatrix[Double]] = {
      val Pair(mP0, mQ0) = svd
      val width = mP0.numColumns
      val sc = mE.sc
      /* Initial version:
      val vsP = (mE.rows zip mP0.rows).map { case Pair(vE, vP0) =>
        val indices = vE.nonZeroIndices
        val len = indices.length
        //val errorsNA = vE.nonZeroValues.map(e => replicate(width, e))
        val mQ0_cut = mQ0(indices)
        val vP1 = FactorsMatrix(sc,
          (mQ0_cut.rows zip vE.nonZeroValues).map { case Pair(vQ0, e) => vQ0 *^ e },
          mQ0.numColumns).reduceByColumns
        val k = IF (len > zeroInt) THEN power(one - gamma2 * lambda7, len) ELSE one
        (vP1 *^ gamma2) +^ (vP0 *^ k)
      }
      TODO:
        There should be lifted version of each operation here.
        Currently, it is made manually
      */
      val indicesLifted: Rep[SRDD[Array[Int]]] = mE.rddIdxs.rdd
      val valuesLifted: Rep[SRDD[Array[Double]]] = mE.rddVals.rdd
      val lenLifted: Rep[SRDD[Int]] = indicesLifted.map { indices: Rep[Array[Int]] => indices.length}

      val mQ0CutLifted: Rep[SRDD[Array[Array[Double]]]] = applyLifted[Array[Double]](mQ0.rddVals.rdd, indicesLifted)

      val factorMatrixLifted: Rep[SRDD[Array[Array[Double]]]] = (mQ0CutLifted zip valuesLifted).map (fun { case Pair(mQ0_cut, values) =>
        (mQ0_cut zip values).map { case Pair(vQ0, e) => vQ0.map { v => v * e}}
      })
      val vP1Lifted: Rep[SRDD[Array[Double]]] = factorMatrixLifted.map ( fun { in: Rep[Array[Array[Double]]] =>
        val numRows = in.length
        array_rangeFrom0(mQ0.numColumns).map { col: Rep[Int] =>
          array_rangeFrom0(numRows).map { row: Rep[Int] => in(row)(col) }.reduce
        }
      })

      val kLifted: Rep[SRDD[Double]] = lenLifted.map( fun { len: Rep[Int] =>
        IF (len > zeroInt) THEN power(one - gamma2 * lambda7, len) ELSE one
      })

      val vsP: Rep[SRDD[Array[Double]]] = (vP1Lifted zip (mP0.rddVals.rdd zip kLifted)).map( fun { case Tuple(vP1, vP0, k) =>
        (vP1 zip vP0).map { case Pair(p1, p0) => p1 * gamma2 + p0 *k}
      })

      FactorsMatrixNew(sc, vsP, width)
    }

    // move to Models DSL
    def calculateMQSpark(mE: Rep[SparkSparseMatrix[Double]], mEt: Rep[SparkSparseMatrix[Double]], mN: Rep[SparkSparseMatrix[Double]],
                    gamma2: DoubleRep, lambda7: DoubleRep)(svdpp: SparkModSVDpp): Rep[SparkDenseMatrix[Double]] = {
      val Tuple(mP0, mQ0, mY0) = svdpp
      val width = mQ0.numColumns
      val sc = mE.sc

      /* Initial version */
      val vsQ0 = mQ0.rows
      val vsN = mN.rows
      val vsYu = vsN.map { vN =>
        val indices = vN.nonZeroIndices
        val len = indices.length
        val res = mY0(indices).reduceByColumns
        val mult = IF (len > zeroInt) (one / Math.sqrt(len.toDouble)) ELSE zero
        res *^ mult
        //IF (len > zeroInt) THEN { res /^ Math.sqrt(len.toDouble) } ELSE zeroVector(width)
      }
      val mYu = FactorsMatrix(sc, vsYu, width)
      val vsQ = (mEt.rows zip vsQ0).map { case Pair(vEt, vQ0) =>
        val indices = vEt.nonZeroIndices
        IF (indices.length > zeroInt) THEN {
          val errorsNA = vEt.nonZeroValues.map(e => ReplicatedVector(width, e))
          val mErr = FactorsMatrix(sc, errorsNA, width)
          val mP0_cut = mP0(indices)
          val mYu_cut = mYu(indices)
          val vQ1 = ((mP0_cut +^^ mYu_cut) *^^ mErr).reduceByColumns
          vQ0 *^ (one - indices.length.toDouble * gamma2 * lambda7) +^ (vQ1 *^ gamma2)
        } ELSE { vQ0 }
      }
      /*TODO:
        There should be lifted version of each operation here.
        Currently, it is made manually
      */
      /*val mYu = {
        val indicesLifted = mN.rddIdxs.rdd
        val lenLifted: Rep[SRDD[Int]] = indicesLifted.map(fun { idxs: Rep[Array[Double]] => idxs.length})
        val multLifted: Rep[SRDD[Double]] = lenLifted.map(fun { len: Rep[Int] => IF(len > zeroInt)(one / Math.sqrt(len.toDouble)) ELSE zero})

        val mY0CutLifted: Rep[SRDD[Array[Array[Double]]]] = applyLifted[Array[Double]](mY0.rddVals.rdd, indicesLifted)
        val mY0ReducedLifted: Rep[SRDD[Array[Double]]] = mY0CutLifted.map { in: Rep[Array[Array[Double]]] =>
          val numRows = in.length
          array_rangeFrom0(mY.numColumns).map { col: Rep[Int] =>
            array_rangeFrom0(numRows).map { row: Rep[Int] => in(row)(col)}.reduce
          }
        }
        val vsYu: Rep[SRDD[Array[Double]]] = (mY0ReducedLifted zip multLifted).map(fun { case Pair(mY0Reduced, mult) =>
          mY0Reduced.map { i: Rep[Double] => i * mult}
        })
        FactorsMatrixNew(sc, vsYu, width)
      }

      val indicesLifted: Rep[SRDD[Array[Int]]] = mEt.rddIdxs.rdd
      val conditions = indicesLifted.map(fun { indices: Rep[Array[Int]] => indices.length > zeroInt})
      */


      FactorsMatrix(sc, vsQ, width)
    }

    // move to Models DSL
    def calculateMYSpark(mE: Rep[SparkSparseMatrix[Double]], mEt: Rep[SparkSparseMatrix[Double]], mN: Rep[SparkSparseMatrix[Double]],
                    gamma2: DoubleRep, lambda7: DoubleRep)(svdpp: SparkModSVDpp): Rep[SparkDenseMatrix[Double]] = {
      val Tuple(mP0, mQ0, mY0) = svdpp
      val c0 = one - gamma2 * lambda7
      val width = mP0.numColumns
      val vsY0 = mY0.rows
      val vsNt = mN.transpose.rows
      val vsE = mE.rows
      val sc = mE.sc
      val coeffs = mE.rows.map(v => power(c0, v.nonZeroValues.length))
      val addDevs = (vsE zip mN.rows.map(v => v.nonZeroIndices.length)).map { case Pair(vE, len) =>
        IF (len > zeroInt) THEN {
          val indices = vE.nonZeroIndices
          val errors  = vE.nonZeroValues
          val errorNA = errors.map(e => replicate(width, e)) // TODO: check if it's OK to discard errors
          // from the formula of updating mY
          val mQ0_cut  = mQ0(indices)
          val c = one / Math.sqrt(len.toDouble)
          FactorsMatrix(sc,
            (mQ0_cut.rows zip errors).map { case Pair(vQc, e) => vQc *^ e },
            width).reduceByColumns *^ c
        } ELSE { zeroVector(width) }
      }
      val mAddDev = FactorsMatrix(sc,addDevs, width)
      val vsY = (vsNt zip vsY0).map { case Pair(vNt, vY0) =>
        val indices = vNt.nonZeroIndices
        val len = indices.length
        IF (len > zeroInt) THEN {
          val k = IF (len > zeroInt) THEN power(c0, len) ELSE one
          val coeffY = coeffs(indices).reduce(Multiply) // TODO MATH: check its usage
          val addDevY = mAddDev(indices).reduceByColumns.items
          (vY0 *^ k) +^ addDevY
        } ELSE { vY0 }
      }
      val mY = FactorsMatrix(sc,vsY, width)
      mY
    }

    def trainSpark(closure: SparkClosure, stddev: DoubleRep)(implicit o: Overloaded1): SparkSVDState = {
      val Tuple(parameters, mR, _) = closure
      val Tuple(_, _, _, _, _, _, width, _) = parameters
      val nUsers = mR.numRows
      val nItems = mR.numColumns
      val mdl0 = initSpark(mR.sc, nUsers, nItems, width, stddev)
      trainSparkInternal(closure, mdl0)
    }

    def trainSparkInternal(closure: SparkClosure, mdl: SparkSVDModel): SparkSVDState = {
      val Tuple(_, mR, _) = closure
      //val start: StateML = StateSVDpp(mdl, oneInt, initialRMSE, flagRunning, one)//.convertTo[AbstractState]
      val meta = Tuple(oneInt, initialRMSE, flagRunning, one)
      val start: SparkSVDState = Pair(mdl, meta)//.convertTo[AbstractState]
      val mu = average(mR)
      val cs0 = mR.rows.map(v => v.nonZeroItems.length)
      val cs0T = mR.countNonZeroesByColumns
      from(start).until(convergedSpark)(stepSpark(closure, cs0, cs0T, mu))
    }

    def SVDppSpark_Train = fun { in: Rep[(ParametersPaired, (RDD[Array[Int]], (RDD[Array[Double]],
      (Int, Double))))] =>
      val Tuple(parametersPaired, idxs, vals, nItems, stddev) = in

      val rddIndexes = SRDDImpl(idxs)
      val rddValues = SRDDImpl(vals)
      val mR = SparkSparseMatrix(RDDCollection(rddIndexes), RDDCollection(rddValues), nItems)

      val closure = Tuple(parametersPaired, mR, mR)
      val stateFinal = trainSpark(closure, stddev)

      val Tuple(res1, res2,res3, res4, res5) = stateFinal
      Pair(res1._1.items.arr, Pair(res1._2.items.arr, Pair(res2, Pair(res3, Pair(res4, res5)))) )
    }

    def SVDppSpark_errorMatrix = fun { in: Rep[(ParametersPaired, (RDD[Array[Int]], (RDD[Array[Double]], (Int, Double))))] =>
      val Tuple(parametersPaired, idxs, vals, nItems,stddev) = in

      val rddIndexes = SRDDImpl(idxs)
      val rddValues = SRDDImpl(vals)
      val mR = SparkSparseMatrix(RDDCollection(rddIndexes), RDDCollection(rddValues), nItems)

      val Tuple(_, _, _, _, _, _, width, _) = parametersPaired
      val nUsers = mR.numRows
      val model0 = initSpark(mR.sc, nUsers, nItems, width, stddev)
      val mu = average(mR)
      val eM = errorMatrixSpark(mR, mR, mu)(model0)
      eM.rows.arr.map {v => v.nonZeroItems.arr}
    }
  }

  def generationConfig(cmpl: SparkScalanCompiler, pack : String = "gen", command: String = null, getOutput: Boolean = false) =
    cmpl.defaultCompilerConfig.copy(scalaVersion = Some("2.10.4"),
      sbt = cmpl.defaultCompilerConfig.sbt.copy(mainPack = Some(s"com.scalan.spark.$pack"),
        resources = Seq("log4j.properties"),
        extraClasses = Seq("com.scalan.spark.method.Methods"),
        toSystemOut = !getOutput,
        commands = if (command == null) cmpl.defaultCompilerConfig.sbt.commands else Seq(command)))

  /*test("SVDppSpark ErrorMatrix Code Gen") {
    val testCompiler = new SparkScalanCompiler with SparkLADslExp with SVDppSpark with CFDslExp {
      self =>
      val sparkContext = globalSparkContext
      val sSparkContext = null
      val conf = null
      val repSparkContext = null
    }
    val compiled1 = compileSource(testCompiler)(testCompiler.SVDppSpark_errorMatrix, "SVDppSpark_errorMatrix", generationConfig(testCompiler, "SVDppSpark_errorMatrix", "package"))
  } */

  test("SVDppSpark Train Code Gen") {
    val testCompiler = new SparkScalanCompiler with SparkLADslExp with SVDppSpark with CFDslExp {
      self =>
      val sparkContext = globalSparkContext
      val sSparkContext = null
      val conf = null
      val repSparkContext = null
    }
    val compiled1 = compileSource(testCompiler)(testCompiler.SVDppSpark_Train, "SVDppSpark_Train", generationConfig(testCompiler, "SVDppSpark_Train", "package"))
  }
}

