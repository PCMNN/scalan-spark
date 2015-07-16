package la

import scalan.ml.{MLDsl}

/**
 * Created by afilippov on 4/15/15.
 */
trait SimpleLASparkTests extends MLDsl with SparkLADsl {

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
    val matrix: Matrix[Double] = CompoundMatrix(RDDCollection(m.map { r: Arr[Double] => DenseVector(Collection(r)) }), v.length)
    val vector: Vector[Double] = DenseVector(Collection(v))
    (matrix * vector).items.arr
  }

  lazy val sdmvm = fun { p: Rep[(SRDD[(Array[Int], Array[Double])], (Int, Array[Double]))] =>
    val Tuple(m, numCols, v) = p
    val idxs = m.map(fun{in => in._1})
    val vals = m.map(fun{in => in._2})
    val matrix: Matrix[Double] = SparkSparseMatrix(RDDCollection(idxs), RDDCollection(vals), numCols)
    val vector: Vector[Double] = DenseVector(Collection(v))
    (matrix * vector).items.arr
  }
}