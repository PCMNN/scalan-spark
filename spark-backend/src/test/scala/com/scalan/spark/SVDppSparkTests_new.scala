package com.scalan.spark

/**
 * Created by afilippov on 5/19/15.
 */

import java.io.File

import com.scalan.spark.backend.SparkScalanCompiler
import la.{SparkLADsl, SparkLADslExp}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.scalatest.BeforeAndAfterAll

import scala.language.reflectiveCalls
import scalan.BaseTests
import scalan.common.OverloadHack.Overloaded1
import scalan.it.ItTestsUtil
import scalan.ml.{CF, CFDslExp}

class SVDppSparkTests_new extends BaseTests with BeforeAndAfterAll with ItTestsUtil { suite =>
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

    type SparkClosure = Rep[(ParametersPaired, (SparkSparseIndexedMatrix[Double], SparkSparseIndexedMatrix[Double]))]

    type SparkSVDModelVal = (RDDIndexedCollection[Double], (DenseVector[Double],
      (SparkDenseIndexedMatrix[Double], (SparkDenseIndexedMatrix[Double], SparkDenseIndexedMatrix[Double]))))

    type SparkSVDModel = Rep[SparkSVDModelVal]

    type SparkSVDState = Rep[(SparkSVDModelVal, (Int, (Double, (Int, Double))))]

    type SparkModSVD = Rep[(SparkDenseIndexedMatrix[Double], SparkDenseIndexedMatrix[Double])]

    type SparkModSVDpp = Rep[(SparkDenseIndexedMatrix[Double], (SparkDenseIndexedMatrix[Double], SparkDenseIndexedMatrix[Double]))]


    def replicate[T: Elem](len: IntRep, v: Rep[T]): Coll[T] = Collection.replicate[T](len, v)
    def ReplicatedVector(len: IntRep, v: DoubleRep): Rep[DenseVector[Double]] = DenseVector(replicate(len, v))
    def zeroVector(sc: Rep[SSparkContext], len: IntRep): Rep[RDDIndexedCollection[Double]] = {
      val arr = SArray.replicate(len, 0.0)
      val arrIdxs = SArray.rangeFrom0(len).map { i:Rep[Int] => i.toLong}
      val rdd: Rep[SRDD[(Long, Double)]] = rddToPairRddFunctions(sc.makeRDD(SSeq(arrIdxs zip arr), sc.defaultParallelism)).partitionBy(SPartitioner.defaultPartitioner(sc.defaultParallelism))
      /*DenseVector*/(RDDIndexedCollection(rdd))
      //DenseVector(Collection.replicate(len, 0.0))
    }

    def RandomMatrix(sc: Rep[SSparkContext], numRows: IntRep, numColumns: IntRep, mean: DoubleRep, stddev: DoubleRep): Rep[SparkDenseIndexedMatrix[Double]] = {
      //val vals = SArray.replicate(numRows, SArray.replicate(numColumns, 0.0))
      val arr = SArray.replicate(numRows, 0)
      val arrIdxs = SArray.rangeFrom0(numRows).map { i: Rep[Int] => i.toLong}
      val rowIndRdd = rddToPairRddFunctions(sc.makeRDD(SSeq(arrIdxs zip arr), sc.defaultParallelism)).partitionBy(SPartitioner.defaultPartitioner(sc.defaultParallelism))

      val rddVals: Rep[SRDD[(Long, Array[Double])]] = rowIndRdd.map(fun { in:Rep[(Long,Int)] => Pair(in._1, /*SArray.replicate(numColumns, zero) }*/ array_randomGaussian(mean, stddev, SArray.replicate(numColumns, zero)) ) })
      SparkDenseIndexedMatrix(RDDIndexedCollection(rddVals), numColumns)
    }

    def RatingsMatrix(rddIndexes: Rep[RDDIndexedCollection[Array[Int]]], rddValues: Rep[RDDIndexedCollection[Array[Double]]], numColumns: IntRep): Rep[SparkSparseIndexedMatrix[Double]] = {
      SparkSparseIndexedMatrix(rddIndexes, rddValues, numColumns)
    }

    def FactorsMatrix(sc: Rep[SSparkContext], rows: Coll[AbstractVector[Double]], numColumns: IntRep): Rep[SparkDenseMatrix[Double]] = {
      val rddValues = RDDCollection(sc.makeRDD(rows.map { vec => vec.items.arr}.seq)) //asRep[RDDCollection[Array[Double]]]
      SparkDenseMatrix(rddValues, numColumns)
    }

    def FactorsMatrixNew(rows: Rep[SRDD[(Long,Array[Double])]], numColumns: IntRep): Rep[SparkDenseIndexedMatrix[Double]] = {
      SparkDenseIndexedMatrix(RDDIndexedCollection(rows), numColumns)
    }

    def average(matrix: Rep[SparkSparseIndexedMatrix[Double]]) = {
      val rdd: Rep[SRDD[(Double,Int)]] = matrix.rddVals.rdd.map(fun{ v: Rep[Array[Double]] => Pair(v.reduce, v.length)})
      val redFun = fun[((Double,Int),(Double,Int)), (Double,Int)] { case Pair(Pair(i11,i12),Pair(i21,i22)) => Pair(i11 + i21, i12 + i22)}

      val reduced: Rep[(Double, Int)] = rdd.fold((0.0,0))(redFun)
      reduced._1 / reduced._2.toDouble

      //val coll = matrix.rows.map{ v => v.nonZeroValues.reduce }
      //coll.reduce / coll.length.toDouble
    }

    def initSpark(sc: Rep[SSparkContext], nUsers: IntRep, nItems: IntRep, width: IntRep, stddev: DoubleRep) = {
      val vBu0 = zeroVector(sc, nUsers) // DenseVector(RDDCollection(sc.makeRDD(SSeq(SArray.replicate(nUsers, 0.0)))))//zeroVector(nUsers)
      val vBi0 = DenseVector(Collection.replicate(nItems, 0.0)) // zeroVector(nItems)
      val mP0 = RandomMatrix(sc,nUsers, width, zero, stddev)
      val mQ0 = RandomMatrix(sc,nItems, width, zero, stddev)
      val mY0 = RandomMatrix(sc,nItems, width, zero, stddev)
      Tuple(vBu0, vBi0, mP0, mQ0, mY0)
    }

    val swapArrIntLongFun = fun {in: Rep[(Long,Array[Int])] => Pair(in._2, in._1)}
    val flatMapWithIdxsFun = fun { in: Rep[(Long, Array[Int])] => SSeq(in._2.map({ v: Rep[Int] => (v.toLong, in._1)})) }
    val resFlatMapFun = fun{ in: Rep[(Long, (ValueType, SSeq[Long]))] =>
      val seq: Rep[SSeq[Long]] = in._3
      val value: Rep[ValueType] = in._2
      seq.map( fun{ i: Rep[Long] => (i,value)} )
    }
    val finalResFun = fun { in:Rep[(Long, (SSeq[ValueType], SSeq[Boolean]))] => in._2.toArray}
    val emptyFun = fun { in: Rep[(Long,Array[Int])] => Pair(in._1, toRep(false))}
    val multiplyEachRowByVectorElemFun = fun { in: Rep[(Array[Array[Double]], Array[Double])] =>
      val Pair(matr, vec) = in
      (matr zip vec).map { case Pair(row, e) => row.map { v => v * e}}
    }

    def stepSpark(closure: SparkClosure, cs0: Rep[RDDIndexedCollection[Int]], cs0T: Vector[Int], mu: DoubleRep)(state: SparkSVDState): SparkSVDState = {
      val Tuple(parameters, mR, mN) = closure
      val Tuple(maxIterations, convergeLimit, gamma1, gamma2, lambda6, lambda7, width, coeffDecrease) = parameters
      val Pair(model0, meta) = state
      val Tuple(vBu0, vBi0, mP0, mQ0, mY0) = model0

      val mE = errorMatrixSpark(mR, mN, mu)(model0)
      //val rmseNew = calculateRMSESpark(mE)

      val Tuple(iteration, rmse, flagRunning, stepDecrease0) = meta
      val rmseNew = rmse //calculateRMSESpark(mE)
      val stepDecrease = coeffDecrease * stepDecrease0

      val stateNew = { /*IF (rmseNew > rmse) THEN {
        Pair(model0, Tuple(iteration, rmse, flagFailure1, stepDecrease))
      } ELSE {*/
        val bl = Tuple(vBu0, vBi0)
        val svd = Tuple(mP0, mQ0)
        val svdpp = Tuple(mP0, mQ0, mY0)
        val Tuple(vBu, vBi) = calculateBaselineSpark(mE, cs0, cs0T, mu, gamma1 * stepDecrease, lambda6)(bl)

        val mQMultipliedReducedLifted: Rep[SRDD[Array[Double]]] = {
          val indicesLifted: Rep[SRDD[(Long, Array[Int])]] = mE.rddIdxs.indexedRdd
          val errorsLifted: Rep[SRDD[Array[Double]]] = mE.rddVals.rdd
          val mQ0CutLifted: Rep[SRDD[Array[Array[Double]]]] = applyLifted(mQ0.rddVals.indexedRdd, indicesLifted)

          val multipliedLifted: Rep[SRDD[Array[Array[Double]]]] = (mQ0CutLifted zip errorsLifted).map(multiplyEachRowByVectorElemFun)
          val res = multipliedLifted.map(fun { in: Rep[Array[Array[Double]]] =>
            val numRows = in.length
            array_rangeFrom0(width).map { col: Rep[Int] =>
              array_rangeFrom0(numRows).map { row: Rep[Int] => in(row)(col)}.reduce
            }
          })
          res.cache
        }
        val mP = calculateMPSpark(mE, gamma2 * stepDecrease, lambda7, mQMultipliedReducedLifted)(svd)
        val mQ = calculateMQSpark(mE, mN, gamma2 * stepDecrease, lambda7)(svdpp)
        val mY = calculateMYSpark(mE, mN, gamma2 * stepDecrease, lambda7, mQMultipliedReducedLifted)(svdpp)

        val modelNew = Tuple(vBu, vBi, mP, mQ, mY)

        val parameters = IF (iteration >= maxIterations) THEN {
          Tuple(iteration, rmseNew, flagOverMaxIter, stepDecrease)
        } ELSE {
          /*IF (rmse - rmseNew < convergeLimit) THEN {
            Tuple(iteration, rmseNew, flagConverged, stepDecrease)
          } ELSE */{
            Tuple(iteration + oneInt, rmseNew, flagRunning, stepDecrease)
          }
        }

        Pair(modelNew, parameters)
      }
      //val stateNew = selectStateSpark(rmse, maxIterations, convergeLimit, coeffDecrease)(state, modelNew)
      stateNew
    }

    def convergedSpark(state: SparkSVDState): BoolRep = {
      val Pair(_, meta) = state
      val Tuple(_, _, exitFlag, _) = meta
      exitFlag !== flagRunning
    }

    def calculateRMSESpark(mE: Rep[SparkSparseIndexedMatrix[Double]]): DoubleRep = {
      val rdd: Rep[SRDD[(Double,Int)]] = mE.rddVals.rdd.map(fun{ v: Rep[Array[Double]] => Pair(v.map{el:Rep[Double] => el * el}.reduce, v.length)})
      // TODO LMS: this code fails in LMS backend (the previous line passes)
      //val coll = mE.rows.flatMap(v => v.nonZeroValues).map(v => v * v)
      val redFun = fun[((Double,Int),(Double,Int)), (Double,Int)] { case Pair(Pair(i11,i12),Pair(i21,i22)) => Pair(i11 + i21, i12 + i22)}

      val reduced: Rep[(Double, Int)] = rdd.fold((0.0,0))(redFun)
      Math.sqrt(reduced._1 / reduced._2.toDouble)
    }
    def calculateBaselineSpark(mE: Rep[SparkSparseIndexedMatrix[Double]], cs0: Rep[RDDIndexedCollection[Int]], cs0T: Vector[Int], mu: Rep[Double],
                          gamma1: Rep[Double], lambda6: Rep[Double])(baseline: Rep[(RDDIndexedCollection[Double],DenseVector[Double])]) : Rep[(RDDIndexedCollection[Double],DenseVector[Double])] = {
      val Pair(vBu0, vBi0) = baseline
      val c0 = one - gamma1 * lambda6
      //val cs = cs0.map(n => power(c0, n))
      val csT = cs0T.items.map(n => power(c0, n))
      val vBu = (mE.rddVals.indexedRdd zip (vBu0.rdd zip cs0.rdd)).map(fun {in: Rep[((Long,Array[Double]), (Double, Int))] =>
        val Pair(Pair(id,arr), Pair(bu0, n)) = in
        Pair(id, arr.reduce* gamma1 + bu0*power(c0,n)) })
      //val vBu = (mE.reduceByRows *^ gamma1) +^ (DenseVector(vBu0) *^ cs)
      val vBi = (mE.reduceByColumns *^ gamma1) +^ (vBi0 *^ csT)
      (RDDIndexedCollection(vBu.cache), DenseVector(vBi.items))
    }

    // move to States DSL
    def selectStateSpark(rmseNew: DoubleRep, maxIterations: IntRep,
                    convergeLimit: DoubleRep, coeffDecrease: DoubleRep)
                   (state: SparkSVDState, modelNew: SparkSVDModel): SparkSVDState = {
      val Pair(model, meta) = state
      val Tuple(iteration, rmse, flagRunning, stepDecrease0) = meta
      val stepDecrease = coeffDecrease * stepDecrease0
      val res = IF (rmseNew > rmse) THEN {
        //printString("Failed to converge any better")
        //printString(state.costValue.toString + " > " + rmse)
        Pair(model, Tuple(iteration, rmse, flagFailure1, stepDecrease))
      } ELSE {
        IF (iteration >= maxIterations) THEN {
          //printString("Reached maximum number of iterations")
          Pair(modelNew, Tuple(iteration, rmseNew, flagOverMaxIter, stepDecrease))
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

    type ValueType = Array[Double]

    def applyTransposedLifted(sc: Rep[SSparkContext], rdd: Rep[SRDD[(Long,ValueType)]], irdd: Rep[SRDD[(Long,Array[Int])]], height: Rep[Int]): Rep[SRDD[Array[ValueType]]] = {
      val rddWithIdxs = rdd

      val flatWithIdxs: Rep[SRDD[(Long, Long)]]= irdd.flatMap(fun { in: Rep[(Long, Array[Int])] => SSeq(in._2.map({ v: Rep[Int] => (in._1, v.toLong)})) })
      val groupped: Rep[SRDD[(Long, SSeq[Long])]] = rddToPairRddFunctions(flatWithIdxs).groupByKey

      val joined: Rep[SRDD[(Long, (ValueType, SSeq[Long]))]] = rddToPairRddFunctions(rddWithIdxs).join(groupped)

      val resFlat:Rep[SRDD[(Long,ValueType)]] = joined.flatMap(resFlatMapFun)
      val empty = sc.makeRDD(SSeq(SArray.rangeFrom0(height)), sc.defaultParallelism ).map ( fun { in: Rep[Int] => Pair(in.toLong, toRep(false))})
      //val empty = irdd.map( emptyFun )

      val res: Rep[SRDD[Array[ValueType]]] = rddToPairRddFunctions(resFlat).groupWithExt(empty).map(finalResFun) //( fun { in:Rep[(Long, (SSeq[ValueType], SSeq[Int]))] => in._2.toArray})

      res //SPairRDDFunctionsImpl(resFlat).groupByKey.map (fun { in: Rep[(Long, SSeq[ValueType])] => in._2.toArray})
    }
    def applyLifted(rdd: Rep[SRDD[(Long,ValueType)]], irdd: Rep[SRDD[(Long,Array[Int])]], needSwap: Boolean = true): Rep[SRDD[Array[ValueType]]] = {
      val rddWithIdxs = rdd

      val flatWithIdxs: Rep[SRDD[(Long, Long)]]= needSwap match {
        //val idxsWithIdxs: Rep[SRDD[(Array[Int], Long)]] = irdd.map(swapArrIntLongFun) //fun{ in => Pair(in._2, in._1)})
        case true => irdd.flatMap(flatMapWithIdxsFun) //(fun { in: Rep[(Array[Int], Long)] => SSeq(in._1.map({ v: Rep[Int] => (v.toLong, in._2)})) })
        case false => irdd.flatMap(fun { in: Rep[(Long, Array[Int])] => SSeq(in._2.map({ v: Rep[Int] => (in._1, v.toLong)})) })
      }

      val groupped: Rep[SRDD[(Long, SSeq[Long])]] = rddToPairRddFunctions(flatWithIdxs).groupByKey

      val joined: Rep[SRDD[(Long, (ValueType, SSeq[Long]))]] = rddToPairRddFunctions(rddWithIdxs).join(groupped)

      val resFlat:Rep[SRDD[(Long,ValueType)]] = joined.flatMap(resFlatMapFun) /*(fun{ in: Rep[(Long, (ValueType, SSeq[Long]))] =>
        val seq: Rep[SSeq[Long]] = in._3
        val value: Rep[ValueType] = in._2
        seq.map( fun{ i: Rep[Long] => (i,value)} )
      })*/

      //val empty = sc.makeRDD(SSeq(SArray.rangeFrom0(numRows.toInt)), sc.defaultParallelism ).map ( fun[Int, (Long,Int)] { in: Rep[Int] => Pair(in.toLong, -1)})
      val empty = irdd.map( emptyFun )

      val res: Rep[SRDD[Array[ValueType]]] = rddToPairRddFunctions(resFlat).groupWithExt(empty).map(finalResFun) //( fun { in:Rep[(Long, (SSeq[ValueType], SSeq[Int]))] => in._2.toArray})

      res //SPairRDDFunctionsImpl(resFlat).groupByKey.map (fun { in: Rep[(Long, SSeq[ValueType])] => in._2.toArray})
    }
    def transposeLocal(matr: Rep[SparkSparseIndexedMatrix[Double]]): Rep[SparkSparseIndexedMatrix[Double]] = {

      val idxs: Rep[SRDD[Array[Int]]] = matr.rddIdxs.rdd
      val vals: Rep[SRDD[(Long,Array[Double])]] = matr.rddVals.indexedRdd
      val sc: Rep[SSparkContext] = matr.sc
      val numRows = matr.numRows
      val numColumns = matr.numColumns


      // TODO: CHeck if Int is OK
      val hostCols = SSeq(SArray.rangeFrom0(numColumns).map { i => (i.toLong,toRep(false))})
      val cols: Rep[SRDD[(Long,Boolean)]] = rddToPairRddFunctions(sc.makeRDD(hostCols, sc.defaultParallelism)).partitionBy(SPartitioner.defaultPartitioner(sc.defaultParallelism))

      val flatValsWithRows: Rep[SRDD[(Long, Double)]] = vals.flatMap( fun { rv: Rep[(Long,Array[Double])] => SSeq(rv._2.map { v: Rep[Double] => (rv._1,v)}) } )
      val flatIdxs: Rep[SRDD[Long]] = idxs.flatMap( fun { a: Rep[Array[Int]] => SSeq(a.map{i => i.toLong}) })
      val zippedFlat: Rep[SRDD[(Long, (Long, Double))]] = flatIdxs zip flatValsWithRows

      val eCols: Rep[SPairRDDFunctionsImpl[Long, Boolean]] =  rddToPairRddFunctions(cols)

      val groupped = eCols.groupWithExt(zippedFlat)

      val el: LElem[(Long,(SSeq[Boolean], SSeq[(Long,Double)]))] = toLazyElem(PairElem(element[Long], PairElem(element[SSeq[Boolean]], element[SSeq[(Long,Double)]])))
      val r: Rep[SRDD[((Long,Array[Int]), (Long, Array[Double]))]] = groupped.map(fun{in: Rep[(Long, (SSeq[Boolean], SSeq[(Long,Double)]))] =>
        val Pair(ind,Pair(_,seq)) = in
        val idxs = seq.map(fun {in: Rep[(Long,Double)] => in._1.toInt}).toArray
        val vals = seq.map(fun {in: Rep[(Long,Double)] => in._2}).toArray
        Pair(Pair(ind, idxs), Pair(ind, vals))
      }(el))

      val result: Rep[SPairRDDFunctionsImpl[(Long,Array[Int]), (Long, Array[Double])]] = rddToPairRddFunctions(r)

      SparkSparseIndexedMatrix(RDDIndexedCollection(result.keys), RDDIndexedCollection(result.values), numRows)
    }

    // move to Datasets DSL? Or better to Models DSL?
    def errorMatrixSpark(mR: Rep[SparkSparseIndexedMatrix[Double]], mN: Rep[SparkSparseIndexedMatrix[Double]], mu: DoubleRep)(model: SparkSVDModel): Rep[SparkSparseIndexedMatrix[Double]] = {
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

      val indicesLifted: Rep[SRDD[(Long,Array[Int])]] = mN.rddIdxs.indexedRdd
      val vrIndicesLifted: Rep[SRDD[(Long,Array[Int])]] = mR.rddIdxs.indexedRdd

      val kLifted: Rep[SRDD[Double]] = indicesLifted.map { indices: Rep[(Long,Array[Int])] =>
        val idx = indices._1
        val k = IF (indices._2.length > zeroInt) THEN { one / Math.sqrt(indices._2.length.toDouble) } ELSE zero
        k
      }

      val mYCutLifted: Rep[SRDD[Array[Array[Double]]]] =  applyLifted(mY.rddVals.indexedRdd, indicesLifted)

      val applyLiftedReduced: Rep[SRDD[Array[Double]]] =   mYCutLifted.map { in: Rep[Array[Array[Double]]] =>
        val numRows = in.length
        val red: Rep[Array[Double]] = array_rangeFrom0(mY.numColumns).map { col: Rep[Int] =>
          array_rangeFrom0(numRows).map { row: Rep[Int] => (in)(row)(col) }.reduce
        }
        red
      }

      val vPvYLifted: Rep[SRDD[Array[Double]]] = (applyLiftedReduced zip (mP.rddVals.rdd zip kLifted)) map ( fun { case Pair(vY, Pair(vP, k)) =>
        val value = (vY zip vP).map { case Pair(y,p) => y * k + p}
        value
      })

      val mQCutLifted: Rep[SRDD[Array[Array[Double]]]] = applyLifted(mQ.rddVals.indexedRdd, vrIndicesLifted)

      val mQLiftedReduced: Rep[SRDD[Array[Double]]] = (mQCutLifted zip vPvYLifted).map (fun { case Pair(mQCut, vPvY) =>
          val value: Rep[Array[Double]] = mQCut.map { row: Rep[Array[Double]] =>
            (row zip vPvY).map { in: Rep[(Double, Double)] => in._1 * in._2}.reduce
          }
          value
      })

      val vBrdd = vBu.rdd
      val vBiBC: Rep[SBroadcast[Collection[Double]]] = mR.sc.broadcast(vBi.items)

      val vsE: Rep[SRDD[(Long, Array[Double])]] = ( mQLiftedReduced zip (vBrdd zip (mR.rddIdxs.indexedRdd zip mR.rddVals.rdd))) map fun({
          case Pair(mQReduced, Pair( bu, Pair(Pair(id,vRidxs), vRvals))) =>
            val vBii = vBiBC.value.apply(Collection(vRidxs)).arr
            val newValues = (vRvals zip (vBii zip mQReduced)).map { case Tuple(r, bi, pq) =>
              r - mu - bu - bi - pq
            }
            Pair(id, newValues)
      })
      RatingsMatrix(mR.rddIdxs, RDDIndexedCollection(vsE.cache), nItems)
    }

    // move to Models DSL
    def calculateMPSpark(mE: Rep[SparkSparseIndexedMatrix[Double]], gamma2: DoubleRep, lambda7: DoubleRep, mQMultipliedReducedLifted: Rep[SRDD[Array[Double]]])(svd: SparkModSVD): Rep[SparkDenseIndexedMatrix[Double]] = {

      val Pair(mP0, mQ0) = svd
      //mP0

      val width = mP0.numColumns
      /* Initial version:
      val sc = mE.sc
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
      val indicesLifted: Rep[SRDD[(Long,Array[Int])]] = mE.rddIdxs.indexedRdd
      /*
      val valuesLifted: Rep[SRDD[Array[Double]]] = mE.rddVals.rdd
      val lenLifted: Rep[SRDD[Int]] = indicesLifted.map { indices: Rep[(Long,Array[Int])] => indices._2.length}

      val mQ0CutLifted: Rep[SRDD[Array[Array[Double]]]] = applyLifted(mQ0.rddVals.indexedRdd, indicesLifted)

      val factorMatrixLifted: Rep[SRDD[Array[Array[Double]]]] = (mQ0CutLifted zip valuesLifted).map(multiplyEachRowByVectorElemFun) /*(fun { case Pair(mQ0_cut, values) =>
        (mQ0_cut zip values).map { case Pair(vQ0, e) => vQ0.map { v => v * e}}
      })  */
      val mQMultipliedReducedLifted: Rep[SRDD[Array[Double]]] = factorMatrixLifted.map /*(reduceByColumnsFun) */( fun { in: Rep[Array[Array[Double]]] =>
        val numRows = in.length
        array_rangeFrom0(mQ0.numColumns).map { col: Rep[Int] =>
          array_rangeFrom0(numRows).map { row: Rep[Int] => in(row)(col) }.reduce
        }
      }) */

      val kLifted: Rep[SRDD[Double]] = indicesLifted.map( fun { row: Rep[(Long, Array[Int])] =>
        val len = row._2.length
        IF (len > zeroInt) THEN power(one - gamma2 * lambda7, len) ELSE one
      })

      val vsP: Rep[SRDD[(Long,Array[Double])]] = (mQMultipliedReducedLifted zip (mP0.rddVals.indexedRdd zip kLifted)).map( fun { case Tuple(vP1, Pair(id,vP0), k) =>
        val value= (vP1 zip vP0).map { case Pair(p1, p0) => p1 * gamma2 + p0 *k}
        Pair(id,value)
      })

      FactorsMatrixNew(vsP.cache, width)
    }

    // move to Models DSL
    def calculateMQSpark(mE: Rep[SparkSparseIndexedMatrix[Double]], mN: Rep[SparkSparseIndexedMatrix[Double]],
                    gamma2: DoubleRep, lambda7: DoubleRep)(svdpp: SparkModSVDpp): Rep[SparkDenseIndexedMatrix[Double]] = {
      val Tuple(mP0, mQ0, mY0) = svdpp
      //mQ0

      val mEt = transposeLocal(mE)
      val width = mQ0.numColumns
      /* Initial version */
      /*
      val sc = mE.sc
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

      FactorsMatrix(sc, vsQ, width)
      } */
      /*TODO:
        There should be lifted version of each operation here.
        Currently, it is made manually
      */
      val mYu = {
        val indicesLifted = mN.rddIdxs.indexedRdd
        val lenLifted: Rep[SRDD[(Long,Int)]] = indicesLifted.map(fun { idxs: Rep[(Long,Array[Int])] => Pair(idxs._1,idxs._2.length)})
        val multLifted: Rep[SRDD[(Long,Double)]] = lenLifted.map(fun { len: Rep[(Long,Int)] => Pair(len._1, IF(len._2 > zeroInt)(one / Math.sqrt(len._2.toDouble)) ELSE zero ) })

        val mY0CutLifted: Rep[SRDD[Array[Array[Double]]]] = applyLifted(mY0.rddVals.indexedRdd, indicesLifted)

        val mY0ReducedLifted: Rep[SRDD[Array[Double]]] = mY0CutLifted.map/*(reduceByColumnsFun) */(fun { in: Rep[Array[Array[Double]]] =>
          val numRows = in.length
          array_rangeFrom0(width).map { col: Rep[Int] =>
            array_rangeFrom0(numRows).map { row: Rep[Int] => in(row)(col)}.reduce
          }
        })
        val vsYu: Rep[SRDD[(Long,Array[Double])]] = (mY0ReducedLifted zip multLifted).map(fun { case Pair(mY0Reduced, Pair(id,mult)) =>
          val value =  mY0Reduced.map { i: Rep[Double] => i * mult}
          Pair(id,value)
        })
        FactorsMatrixNew(vsYu, width)
      }

      val vsQ0: Rep[SRDD[Array[Double]]] = mQ0.rddVals.rdd

      val vsQ: Rep[SRDD[(Long,Array[Double])]] = {
        val indicesLifted: Rep[SRDD[(Long,Array[Int])]] = mEt.rddIdxs.indexedRdd
        val nonZeroValsLifted: Rep[SRDD[Array[Double]]] = mEt.rddVals.rdd

        val mP0CutLifted: Rep[SRDD[Array[Array[Double]]]] = applyLifted(mP0.rddVals.indexedRdd, indicesLifted)
        val mYuCutLifted: Rep[SRDD[Array[Array[Double]]]] = applyLifted(mYu.rddVals.indexedRdd, indicesLifted)

        val plused: Rep[SRDD[Array[Array[Double]]]] = (mP0CutLifted zip mYuCutLifted).map (fun { in: Rep[(Array[Array[Double]], Array[Array[Double]])] =>
          (in._1 zip in._2).map { arrs: Rep[(Array[Double], Array[Double])] =>
            (arrs._1 zip arrs._2).map { vs: Rep[(Double, Double)] => vs._1 + vs._2}
        }
        })
        val multiplied :Rep[SRDD[Array[Array[Double]]]] = (plused zip nonZeroValsLifted).map(multiplyEachRowByVectorElemFun)/*( fun { in: Rep[(Array[Array[Double]], Array[Double])] =>
          (in._1 zip in._2).map { in: Rep[(Array[Double], Double)] => in._1.map { v => v* in._2} }
        }) */

        val vQ1Lifted: Rep[SRDD[Array[Double]]] = multiplied.map/*(reduceByColumnsFun) */( fun { in: Rep[Array[Array[Double]]] =>
          val numRows = in.length
          array_rangeFrom0(width).map { col: Rep[Int] =>
            array_rangeFrom0(numRows).map { row: Rep[Int] => in(row)(col)}.reduce
          }
        } )

        val multsLifted: Rep[SRDD[(Long,Double)]] = indicesLifted.map(fun { case Pair(id, arr) => Pair(id,(one - arr.length.toDouble * gamma2 * lambda7)) })

        val res: Rep[SRDD[(Long,Array[Double])]] = (vsQ0 zip (vQ1Lifted zip multsLifted)).map ( fun { case Tuple(vQ0, vQ1, id,mult) =>
          val value = (vQ0 zip vQ1).map { case Tuple(v0,v1) => v0 * mult + v1 * gamma2}
          Pair(id, value)
        })

        res
      }

      FactorsMatrixNew(vsQ.cache, width)
    }

    // move to Models DSL
    def calculateMYSpark(mE: Rep[SparkSparseIndexedMatrix[Double]], mN: Rep[SparkSparseIndexedMatrix[Double]],
                    gamma2: DoubleRep, lambda7: DoubleRep, mQMultipliedReducedLifted: Rep[SRDD[Array[Double]]])(svdpp: SparkModSVDpp): Rep[SparkDenseIndexedMatrix[Double]] = {
      val Tuple(mP0, mQ0, mY0) = svdpp
     //mY0
      val c0 = one - gamma2 * lambda7
      val width = mY0.numColumns // TODO: check!
      /*
      val vsY0 = mY0.rows
      val vsNt = mNt.rows
      val vsE = mE.rows
      val sc = mE.sc

      val addDevs = (vsE zip mN.rows.map(v => v.nonZeroIndices.length)).map { case Pair(vE, len) =>
        IF (len > zeroInt) THEN {
          val indices = vE.nonZeroIndices
          val errors  = vE.nonZeroValues
          // Commented by A.Filippov.
          // val errorNA = errors.map(e => replicate(width, e)) // TODO: check if it's OK to discard errors
          // from the formula of updating mY
          val mQ0_cut  = mQ0(indices)
          val c = one / Math.sqrt(len.toDouble)
          FactorsMatrix(sc,
            (mQ0_cut.rows zip errors).map { case Pair(vQc, e) => vQc *^ e },
            width).reduceByColumns *^ c
        } ELSE { zeroVector(width) }
      }

      */
      val addDevs: Rep[SRDD[(Long,Array[Double])]] = {
        val indicesLifted: Rep[SRDD[(Long,Array[Int])]] = mE.rddIdxs.indexedRdd
        /*val errorsLifted: Rep[SRDD[Array[Double]]] = mE.rddVals.rdd
        val mQ0CutLifted: Rep[SRDD[Array[Array[Double]]]] = applyLifted(mQ0.rddVals.indexedRdd, indicesLifted)

        val multipliedLifted: Rep[SRDD[Array[Array[Double]]]] = (mQ0CutLifted zip errorsLifted).map(multiplyEachRowByVectorElemFun)
        val mQMultipliedReducedLifted: Rep[SRDD[Array[Double]]] = multipliedLifted.map/*(reduceByColumnsFun) */( fun { in: Rep[Array[Array[Double]]] =>
            val numRows = in.length
            array_rangeFrom0(width).map { col: Rep[Int] =>
              array_rangeFrom0(numRows).map { row: Rep[Int] => in(row)(col)}.reduce
            }
          } )
        */
        // Fused manually
        //val cLifted: Rep[SRDD[Double]] = indicesLifted.map( fun { indices: Rep[Array[Int]] => IF ( indices.length > zeroInt) THEN one / Math.sqrt(indices.length.toDouble) ELSE zero} )
        val res: Rep[SRDD[(Long,Array[Double])]] = (mQMultipliedReducedLifted zip /*cLifted*/indicesLifted).map (fun { case Tuple(r, id, indices) /*in: Rep[(Array[Double], Double)]*/ =>
          val c = IF ( indices.length > zeroInt) THEN one / Math.sqrt(indices.length.toDouble) ELSE zero
          Pair(id, r.map { v => v*c})
        })
        res
      }
      val mAddDev = FactorsMatrixNew(addDevs, width)

      /*val vsY = (vsNt zip vsY0).map { case Pair(vNt, vY0) =>
        val indices = vNt.nonZeroIndices
        val len = indices.length
        IF (len > zeroInt) THEN {
          val k = IF (len > zeroInt) THEN power(c0, len) ELSE one
          val addDevY = mAddDev(indices).reduceByColumns.items
          (vY0 *^ k) +^ addDevY
        } ELSE { vY0 }
      }  */
      val vsY:  Rep[SRDD[(Long,Array[Double])]] = {
        // Fused manually
        val mAddDevCutLifted: Rep[SRDD[Array[Array[Double]]]] = applyTransposedLifted(mN.sc, mAddDev.rddVals.indexedRdd, mN.rddIdxs.indexedRdd, mN.numColumns)

        val addDevYLifted: Rep[SRDD[(Double,Array[Double])]] = mAddDevCutLifted.map/*(reduceByColumnsFun) */( fun { in: Rep[Array[Array[Double]]] =>
          val numRows = in.length
          val arr = array_rangeFrom0(width).map { col: Rep[Int] =>
            array_rangeFrom0(numRows).map { row: Rep[Int] => in(row)(col)}.reduce
          }
          val k = IF (numRows > zeroInt) THEN power(c0,numRows) ELSE one
          Pair(k, arr)
        } )

        val res: Rep[SRDD[(Long,Array[Double])]] = (mY0.rddVals.indexedRdd zip (/*kLifted zip */addDevYLifted)).map ( fun { case Pair(Pair(id,vY0), Pair(k, addDevY)) =>
          val value = (vY0 zip addDevY).map { in: Rep[(Double, Double)] => in._1 * k + in._2}
          Pair(id,value)
        })
        res
      }
      val mY = FactorsMatrixNew(vsY.cache, width)
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
      val cs0 = RDDIndexedCollection(mR.rddIdxs.indexedRdd.map(fun {v => (v._1,v._2.length)}) )
      val cs0T = mR.countNonZeroesByColumns
      val stateFinal = from(start).until(convergedSpark)(stepSpark(closure, cs0, cs0T, mu))
      //val Tuple(res1, res2,res3, res4, res5) = stateFinal
      val Pair(model, metaNew) = stateFinal
      val mE = errorMatrixSpark(mR, mR, mu)(stateFinal._1)
      val rmseNew = calculateRMSESpark(mE)
      val Tuple(iteration, rmse, flag, stepDecrease) = metaNew
      (model, Tuple(iteration, rmseNew, flag, stepDecrease))
    }

    def SVDppSpark_Train = fun { in: Rep[(ParametersPaired, (RDD[(Long,Array[Int])], (RDD[(Long,Array[Double])],
      (Int, Double))))] =>
      val Tuple(parametersPaired, idxs, vals, nItems, stddev) = in

      val rddIndexes = SRDDImpl(idxs)
      val rddValues = SRDDImpl(vals)
      val mR = SparkSparseIndexedMatrix(RDDIndexedCollection(rddIndexes), RDDIndexedCollection(rddValues), nItems)

      val closure = Tuple(parametersPaired, mR, mR)
      val stateFinal = trainSpark(closure, stddev)
      val Tuple(res1, res2,res3, res4, res5) = stateFinal

      Pair(res1._1.arr, Pair(res1._2.items.arr, Pair(res2, Pair(res3, Pair(res4, res5)))) )
    }

    def SVDppSpark_errorMatrix = fun { in: Rep[(ParametersPaired, (RDD[(Long,Array[Int])], (RDD[(Long,Array[Double])], (Int, Double))))] =>
      val Tuple(parametersPaired, idxs, vals, nItems,stddev) = in

      val rddIndexes = SRDDImpl(idxs)
      val rddValues = SRDDImpl(vals)
      val mR = SparkSparseIndexedMatrix(RDDIndexedCollection(rddIndexes), RDDIndexedCollection(rddValues), nItems)

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

  /*test("SVDppSpark_ErrorMatrix_new Code Gen") {
    val testCompiler = new SparkScalanCompiler with SparkLADslExp with SVDppSpark with CFDslExp {
      self =>
      val sparkContext = globalSparkContext
      val sSparkContext = null
      val conf = null
      val repSparkContext = null
    }
    val compiled1 = compileSource(testCompiler)(testCompiler.SVDppSpark_errorMatrix, "SVDppSpark_errorMatrix_new", generationConfig(testCompiler, "SVDppSpark_errorMatrix_new", "package"))
  } */

  test("SVDppSpark_Train_new Code Gen") {
    val testCompiler = new SparkScalanCompiler with SparkLADslExp with SVDppSpark with CFDslExp {
      self =>
      val sparkContext = globalSparkContext
      val sSparkContext = null
      val conf = null
      val repSparkContext = null
    }
    val compiled1 = compileSource(testCompiler)(testCompiler.SVDppSpark_Train, "SVDppSpark_Train_new", generationConfig(testCompiler, "SVDppSpark_Train_new", "package"))
  }
}

