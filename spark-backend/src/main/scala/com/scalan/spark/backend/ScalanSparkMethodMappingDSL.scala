package com.scalan.spark.backend

import org.apache.spark.rdd.{RDD, PairRDDFunctions}

import scalan.compilation.language._

trait ScalanSparkMethodMappingDSL extends MethodMappingDSL {

  trait SparkScalanTags extends MappingTags {

    import scala.reflect.runtime.universe.typeOf

    val tyUnit = typeOf[Unit]
    val tyRDD = typeOf[RDD[_]]
    val tyPairRDDFunctions = typeOf[PairRDDFunctions[_, _]]

    val scalanSpark = new Library("", Array( """  "org.apache.spark" %% "spark-core" % "1.2.0" """)) {
      val scalanSparkPack = new Pack("scalan.spark") {
        val famPairRDDFunctionssAbs = new Family('PairRDDFunctionss) {
          val pairRDDFunctions = new ClassType('SPairRDDFunctions, TyArg('A)) {
            val reduceByKey = Method('reduceByKey, tyPairRDDFunctions)
            val countByKey = Method('countByKey, tyPairRDDFunctions)
            val foldByKey = Method('foldByKey, tyPairRDDFunctions)
            val wrappedValueOfBaseType = Method('wrappedValueOfBaseType, typeOf[scalan.Elems#Element[_]])
          }
        }
        val famRDDsAbs = new Family('RDDsAbs) {
          val sRDDImpl = new ClassType('SRDDImpl, TyArg('A)) {
            val fold = Method('fold, tyUnit)
            val zipSafe = Method('zipSafe, tyRDD)
          }
        }
        val famRDDs = new Family('RDDs) {
          val sRDD = new ClassType('SRDD, TyArg('A)) {
            val fold = Method('fold, tyUnit)
            val zipSafe = Method('zipSafe, tyRDD)
          }
        }
        val sparkContexts = new Family('SparkContexts) {
          val sSparkContextCompanion = new ClassType('SSparkContextCompanion) {
            val apply = Method('apply, typeOf[scalan.spark.impl.SparkConfsAbs#SSparkConfImpl])
          }
        }
      }
      val scalanSparkPackImpl = new Pack("scalan.spark.impl") {
        val famRDDsAbs = new Family('RDDsAbs) {
          val sRDDImpl = new ClassType('SRDDImpl, TyArg('A)) {
            val fold = Method('fold, tyUnit)
            val zipSafe = Method('zipSafe, tyRDD)
            val partitionBy = Method('partitionBy, tyRDD)
          }
        }
        val famPartitionersAbs = new Family('PartitionersAbs) {
          val sPartitionerCompanionAbs = new ClassType('SPartitionerCompanionAbs) {
            val defaultPartitioner = Method('defaultPartitioner, typeOf[org.apache.spark.Partitioner])
          }
          val sPartitionerImpl = new ClassType('SPartitionerImpl, TyArg('A)) {
            val wrappedValueOfBaseType = Method('wrappedValueOfBaseType, typeOf[org.apache.spark.Partitioner])
          }
        }
        val famPairRDDFunctionssAbs = new Family('PairRDDFunctionssAbs) {
          val pairRDDFunctions = new ClassType('SPairRDDFunctionsImpl, TyArg('A)) {
            val reduceByKey = Method('reduceByKey, tyPairRDDFunctions)
            val countByKey = Method('countByKey, tyPairRDDFunctions)
            val foldByKey = Method('foldByKey, tyPairRDDFunctions)
            val groupByKey = Method('groupByKey, tyPairRDDFunctions)
            val groupWithExt = Method('groupWithExt, tyPairRDDFunctions)
            val wrappedValueOfBaseType = Method('wrappedValueOfBaseType, typeOf[scalan.Elems#Element[_]])
          }
        }
        val famRDDs = new Family('RDDs) {
          val sRDD = new ClassType('SRDD, TyArg('A)) {
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
      val scalanCollections = new Pack("scalan.collections") {
          val seqsFam = new Family('Seqs) {
            val sSeq = new ClassType('SSeq, TyArg('A)) {
              val map = Method('map, typeOf[Seq[_]])
            }
          }
      }
    }
  }

  new ScalaMappingDSL with SparkScalanTags {

    val basic = new ScalaLib() {
      val noMethodWrapper = ScalaFunc(Symbol(""))(true)
      val noMethod = ScalaFunc(Symbol(""))()
    }

    val testMethod = new ScalaLib("", "com.scalan.spark.method.Methods") {
      val reduceByKey = ScalaFunc('reduceByKey)(true)
      val countByKey = ScalaFunc('countByKey)(true)
      val foldByKey = ScalaFunc('foldByKey)(true)
      val groupByKey = ScalaFunc('groupByKey)(true)
      val fold = ScalaFunc('fold)(true)
      val zipSafe = ScalaFunc('zipSafe)(true)
      val fromList = ScalaFunc('fromList)()
      val newSeq = ScalaFunc('newSeq)()
      val seqMap = ScalaFunc('seqMap)(true)
      val groupWithExt = ScalaFunc('groupWithExt)(true)
      val defaultPartitioner = ScalaFunc('defaultPartitioner)()
      val partitionBy = ScalaFunc('partitionBy)(true)
    }
    val seq = new ScalaLib(pack = "scala.collection") {
      val empty = ScalaFunc(Symbol("Seq.empty"))()
      val single = ScalaFunc(Symbol("Seq"))()
    }

    val commonMethods = new ScalaLib(pack = "com.scalan.spark.backend.bounds.gen.CommonMethods") {
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
        scalanSpark.scalanSparkPack.famPairRDDFunctionssAbs.pairRDDFunctions.countByKey -> testMethod.countByKey,
        scalanSpark.scalanSparkPack.famPairRDDFunctionssAbs.pairRDDFunctions.foldByKey -> testMethod.foldByKey,
        scalanSpark.scalanSparkPack.famRDDsAbs.sRDDImpl.fold -> testMethod.fold,
        scalanSpark.scalanSparkPackImpl.famRDDsAbs.sRDDImpl.fold -> testMethod.fold,
        scalanSpark.scalanSparkPackImpl.famRDDsAbs.sRDDImpl.partitionBy -> testMethod.partitionBy,
        scalanSpark.scalanSparkPack.famRDDsAbs.sRDDImpl.zipSafe -> testMethod.zipSafe,
        scalanSpark.scalanSparkPackImpl.famRDDsAbs.sRDDImpl.zipSafe -> testMethod.zipSafe,
        scalanSpark.scalanSparkPack.famPairRDDFunctionssAbs.pairRDDFunctions.wrappedValueOfBaseType -> basic.noMethodWrapper,
        scalanSpark.scalanSparkPackImpl.famPairRDDFunctionssAbs.pairRDDFunctions.groupWithExt -> testMethod.groupWithExt,
        scalanSpark.scalanSparkPackImpl.famPairRDDFunctionssAbs.pairRDDFunctions.groupByKey -> testMethod.groupByKey,
        scalanSpark.scalanSparkPack.famRDDs.sRDD.fold -> testMethod.fold,
        scalanSpark.scalanSparkPack.famRDDs.sRDD.zipSafe -> testMethod.zipSafe,
        scalanSpark.scalanSparkPack.famPairRDDFunctionssAbs.pairRDDFunctions.reduceByKey -> testMethod.reduceByKey,
        scalanSpark.scalanSparkPackImpl.famPartitionersAbs.sPartitionerCompanionAbs.defaultPartitioner -> testMethod.defaultPartitioner,
        scalanSpark.scalanSparkPackImpl.famPartitionersAbs.sPartitionerImpl.wrappedValueOfBaseType -> basic.noMethodWrapper,
        scalanComunity.scalanColectionsImp.seqsAbs.sSeqCompanionAbs.empty -> seq.empty,
        scalanComunity.scalanColectionsImp.seqsAbs.sSeqCompanionAbs.fromList -> testMethod.fromList,
        scalanComunity.scalanColectionsImp.seqsAbs.sSeqCompanionAbs.single -> seq.single,
        scalanComunity.scalanColectionsImp.seqsAbs.sSeqCompanionAbs.apply -> testMethod.newSeq,
        scalanComunity.scalanColectionsImp.seqsAbs.sSeqImpl.wrappedValueOfBaseType -> basic.noMethodWrapper,
        scalanComunity.scalanCollections.seqsFam.sSeq.map -> testMethod.seqMap
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

