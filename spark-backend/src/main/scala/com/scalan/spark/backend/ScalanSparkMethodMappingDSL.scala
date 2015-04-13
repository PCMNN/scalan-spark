package com.scalan.spark.backend

import org.apache.spark.rdd.PairRDDFunctions

import scalan.compilation.language._

trait ScalanSparkMethodMappingDSL extends MethodMappingDSL {

  trait TabuSearchTags extends MappingTags {

    import scala.reflect.runtime.universe.typeOf

    val tyUnit = typeOf[Unit]

    val tyPairRDDFunctions = typeOf[PairRDDFunctions[_, _]]

    val scalanSpark = new Library("", Array(""" "com.huawei.scalan" %% "tabusearch" % "1.0" """)) {
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
      }
    }
  }

  new ScalaMappingDSL with TabuSearchTags {
    val testMethod = new ScalaLib("", "com.scalan.spark.method.Methods") {
      val reduceByKey = ScalaFunc('reduceByKey)(true)
      val fold = ScalaFunc('fold)(true)
    }

    val scala2Scala = {
      import scala.language.reflectiveCalls

      Map(
        scalanSpark.scalanSparkPack.famPairRDDFunctionssAbs.pairRDDFunctions.reduceByKey -> testMethod.reduceByKey,
        scalanSpark.scalanSparkPack.famRDDsAbs.sRDDImpl.fold -> testMethod.fold
      )
    }

    val mapping = new ScalaMapping {
      val functionMap = scala2Scala
    }
  }
}

