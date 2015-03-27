package com.scalan.spark.backend

import org.apache.spark.rdd.PairRDDFunctions

import scala.collection.mutable
import scalan.compilation.language._

trait ScalanSparkMethodMapping extends MethodMapping {

  trait TabuSearchConf extends LanguageConf {

    import scala.reflect.runtime.universe.typeOf

    val tyUnit = typeOf[Unit]

    val tyPairRDDFunctions = typeOf[PairRDDFunctions[_, _]]

    val scalanSpark = new Library("", Array(""" "com.huawei.scalan" %% "tabusearch" % "1.0" """)) {
      val scalanSparkPack = new Pack("scalan.spark.impl") {
        val famPairRDDFunctionssAbs = new Family('PairRDDFunctionssAbs) {
          val pairRDDFunctions = new ClassType('SPairRDDFunctionsImpl, 'PA, TyArg('A)) {
            val reduceByKey = Method('reduceByKey, tyPairRDDFunctions)
          }
        }
        val famRDDsAbs = new Family('RDDsAbs) {
          val sRDDImpl = new ClassType('SRDDImpl, 'PA, TyArg('A)) {
            val fold = Method('fold, tyUnit)
          }
        }
      }
    }
  }

  new ScalaLanguage with TabuSearchConf {
    val testMethod = new ScalaLib("", "com.scalan.spark.method.Methods") {
      val reduceByKey = ScalaFunc('reduceByKey)(true)
      val fold = ScalaFunc('fold)(true)
    }

    val scala2Scala = {
      import scala.language.reflectiveCalls

      mutable.Map(
        scalanSpark.scalanSparkPack.famPairRDDFunctionssAbs.pairRDDFunctions.reduceByKey -> testMethod.reduceByKey,
        scalanSpark.scalanSparkPack.famRDDsAbs.sRDDImpl.fold -> testMethod.fold
      )
    }

    val backend = new ScalaBackend {
      val functionMap = scala2Scala
    }
  }
}

