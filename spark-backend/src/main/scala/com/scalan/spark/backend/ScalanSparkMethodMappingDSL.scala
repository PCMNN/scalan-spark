package com.scalan.spark.backend

import org.apache.spark.rdd.PairRDDFunctions

import scalan.compilation.language._

trait ScalanSparkMethodMappingDSL extends MethodMappingDSL {

  trait SparkScalanTags extends MappingTags {

    import scala.reflect.runtime.universe.typeOf

    val tyUnit = typeOf[Unit]

    val tyPairRDDFunctions = typeOf[PairRDDFunctions[_, _]]

    val scalanSpark = new Library("", Array("""  "org.apache.spark" %% "spark-core" % "1.2.0" """)) {
      val scalanSparkPack = new Pack("scalan.spark.impl") {
        val famPairRDDFunctionssAbs = new Family('PairRDDFunctionssAbs) {
          val pairRDDFunctions = new ClassType('SPairRDDFunctionsImpl, TyArg('A)) {
            val reduceByKey = Method('reduceByKey, tyPairRDDFunctions)
          }
        }
        val famRDDsAbs = new Family('RDDsAbs) {
          val sRDDImpl = new ClassType('SRDDImpl, TyArg('A)) {
            val fold = Method('fold, tyUnit)
          }
        }
        val sparkContexts = new Family('SparkContexts) {
          val sSparkContextCompanion = new ClassType('SSparkContextCompanion) {
            val apply = Method('apply, typeOf[scalan.spark.impl.SparkConfsAbs#SSparkConfImpl])
          }
        }
      }
    }

    val scalanComunity = new Library() {
      val scalanColectionsImp = new Pack("scalan.collections.impl") {
        val seqsAbs = new Family('SeqsAbs) {
          val sSeqCompanionAbs = new ClassType('SSeqCompanionAbs, TyArg('A)) {
            val empty = Method('empty, typeOf[scalan.Elems#Element[_]])
            val single = Method('single, typeOf[Seq[_]])
            val apply = Method('apply, typeOf[Seq[_]])
            val fromList = Method('fromList, typeOf[scalan.collections.Seqs#SSeq[_]], MethodArg(typeOf[List[_]]))
          }
          val sSeqImpl = new ClassType('SSeqImpl, TyArg('A)) {
            val wrappedValueOfBaseType = Method('wrappedValueOfBaseType, typeOf[scalan.Elems#Element[_]])
          }
        }
      }
    }
  }

  new ScalaMappingDSL with SparkScalanTags {
    val testMethod = new ScalaLib("", "com.scalan.spark.method.Methods") {
      val reduceByKey = ScalaFunc('reduceByKey)(true)
      val fold = ScalaFunc('fold)(true)
    }
    val seq = new ScalaLib(pack = "scala.collection") {
      val empty = ScalaFunc(Symbol("Seq.empty"))()
      val single = ScalaFunc(Symbol("Seq"))()
    }

    val commonMethods = new ScalaLib(pack = "com.scalan.spark.backend.bounds.gen.CommonMethods") {
      val fold = ScalaFunc('fold)()
      val fromList = ScalaFunc('fromList)()
      val reduceByKey = ScalaFunc('reduceByKey)(true)
      val newSeq = ScalaFunc('newSeq)()
      //val single = ScalaFunc('single)()
      val sparkContext = ScalaFunc('sparkContext)()
    }

    val fValMethods = new ScalaLib(pack = "com.scalan.spark.backend.bounds.gen.FValMethods") {
      val farg = ScalaFunc('farg)(true)
      val arg = ScalaFunc('arg)(true)
      val first = ScalaFunc('f)(true)
      val plus = ScalaFunc('$plus)(true)
      val checkBounds = ScalaFunc('checkBounds)(true)
    }

    val sparkClasses = new ScalaLib() {
      val gridDirection = ScalaFunc(Symbol("acp.GridDirection"))()
      val dArrayPartitioner = ScalaFunc(Symbol("acp.DArrayPartitioner"))()
      val pairRDDFunctions = ScalaFunc(Symbol("org.apache.spark.rdd.PairRDDFunctions"))()
      val sparkConf = ScalaFunc(Symbol("org.apache.spark.SparkConf"))()
      val sparkContext = ScalaFunc(Symbol("org.apache.spark.SparkContext"))()
      val rdd = ScalaFunc(Symbol("org.apache.spark.RDD"))()
    }

    val main = new ScalaLib() {
      val Seq = ScalaFunc(Symbol("scala.collection.Seq[_]"))()
    }

    val scala2Scala = {
      import scala.language.reflectiveCalls

      Map(
        scalanSpark.scalanSparkPack.famPairRDDFunctionssAbs.pairRDDFunctions.reduceByKey -> testMethod.reduceByKey,
        scalanSpark.scalanSparkPack.famRDDsAbs.sRDDImpl.fold -> testMethod.fold,
        scalanSpark.scalanSparkPack.famPairRDDFunctionssAbs.pairRDDFunctions.reduceByKey -> commonMethods.reduceByKey,
        scalanComunity.scalanColectionsImp.seqsAbs.sSeqCompanionAbs.empty -> seq.empty,
        scalanComunity.scalanColectionsImp.seqsAbs.sSeqCompanionAbs.fromList -> commonMethods.fromList,
        scalanComunity.scalanColectionsImp.seqsAbs.sSeqCompanionAbs.single -> seq.single,
        scalanComunity.scalanColectionsImp.seqsAbs.sSeqCompanionAbs.apply -> commonMethods.newSeq,
        scalanSpark.scalanSparkPack.sparkContexts.sSparkContextCompanion.apply -> commonMethods.sparkContext
      )
    }

    val mapping = new ScalaMapping {
      val functionMap = scala2Scala
      override val classMap = Map[Class[_], ScalaFunc](
        classOf[scalan.spark.SparkConfs#SSparkConf] -> sparkClasses.sparkConf,
        classOf[scalan.spark.SparkContexts#SSparkContext] -> sparkClasses.sparkContext,
        classOf[scalan.spark.PairRDDFunctionss#SPairRDDFunctions[_, _]] -> sparkClasses.pairRDDFunctions,
        classOf[scalan.spark.RDDs#SRDD[_]] -> sparkClasses.rdd,
        classOf[scalan.collections.Seqs#SSeq[_]] -> main.Seq
      )
    }
  }
}

