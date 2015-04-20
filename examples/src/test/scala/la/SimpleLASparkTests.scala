package la

import scalan.ml.{ExampleSVDpp, MLDsl}
import scalan.spark.SparkDsl
import scalan.spark.collections.RDDCollectionsDsl

/**
 * Created by afilippov on 4/15/15.
 */
trait SimpleLASparkTests extends MLDsl with SparkDsl with RDDCollectionsDsl with ExampleSVDpp {

  def dvDotDV(in1: Coll[Int], in2: Coll[Int]) = {
    val (vector1, vector2): (Vector[Int], Vector[Int]) = (DenseVector(in1), DenseVector(in2))
    vector1 dot vector2
  }

  def svDotDV(in1_1: Coll[Int], in1_2: Coll[Int], in2: Coll[Int]) = {
    val vector1: Vector[Int] = SparseVector(in1_1, in1_2, in2.length)
    val vector2: Vector[Int] = DenseVector(in2)
    vector1 dot vector2
  }

  lazy val funDvDotDv = fun { in: Rep[(SRDD[Int], SRDD[Int])] =>
    val in1 = RDDCollection(in._1)
    val in2 = RDDCollection(in._2)
    dvDotDV(in1, in2)
  }

  lazy val funSvDotDv = fun { in: Rep[((SRDD[Int], SRDD[Int]), SRDD[Int])] =>
    val in1_1 = RDDCollection(in._1._1)
    val in1_2 = RDDCollection(in._1._2)
    val in2 = RDDCollection(in._2)
    svDotDV(in1_1, in1_2, in2)
  }

  lazy val ddmvm = fun { p: Rep[(SRDD[Array[Double]], Array[Double])] =>
    val Pair(m, v) = p
    val matrix: Matrix[Double] = RowMajorDirectMatrix(RDDCollection(m.map { r: Arr[Double] => DenseVector(Collection(r)) }))
    val vector: Vector[Double] = DenseVector(Collection(v))
    (matrix * vector).items.arr
  }

  lazy val trainAndTestCF = fun { in: Rep[(ParametersPaired, (Array[(Int, Double)], (Array[(Int, Int)],
    (Array[(Int, Double)], (Array[(Int, Int)],
      (Int, Double))))))] =>
    val Tuple(parametersPaired, arrFlat, segsArr, arrTFlat, segsTArr, nItems, stddev) = in
    val nColl: NColl[(Int, Double)] = NestedCollection(Collection(arrFlat), CollectionOfPairs(segsArr))
    val mR: Dataset1 = RowMajorSparseMatrix.fromNColl(nColl, nItems)
    val nCollT: NColl[(Int, Double)] = NestedCollection(Collection(arrTFlat), CollectionOfPairs(segsTArr))
    val mT: Dataset1 = RowMajorSparseMatrix.fromNColl(nCollT, nItems)
    val params = ParametersSVDpp.init(parametersPaired)
    val data = DatasetCF(mR, mR)
    val closure1 = Tuple(parametersPaired, mR, mR)
    val stateFinal = train(closure1, stddev)
    val rmse = predict(mT, mT, stateFinal.model)
    //printString("Cross-validating RMSE: " + rmse)
    rmse
  }

  lazy val rmse = fun { in: Rep[(SRDD[Array[(Int, Double)]], Int)] =>
    val Tuple(arrs, nItems) = in
    val idxs = arrs.map { r: Arr[(Int,Double)] => r.map{_._1}}
    val vals = arrs.map { r: Arr[(Int,Double)] => r.map{_._2}}

    val rows = (idxs zip vals).map{ r: Rep[(Array[Int], Array[Double])] => SparseVector(Collection(r._1), Collection(r._2), nItems)}
    val matrix: Matrix[Double] = RowMajorSparseMatrix(RDDCollection(rows), nItems)
    calculateRMSE(matrix)
  }

  lazy val flatMapDomain = fun  { in: Rep[SRDD[Array[Array[Double]]]] =>
    val rows = RDDCollection(in).flatMap { r: Rep[Array[Array[Double]]] => Collection(r.map { r1: Rep[Array[Double]] => DenseVector(Collection(r1)) }) }
    val matrix: Matrix[Double] = RowMajorDirectMatrix(rows)
    calculateRMSE(matrix)
  }
}