package com.scalan.spark

import scalan.compilation.language._


trait ScalanSparkMethodMapping extends MethodMapping {

  trait ScalanSparkConf extends LanguageConf {

    import scala.reflect.runtime.universe.typeOf

    val sparkLib = new Library("spark-core_2.10-1.2.0.jar") {
    }
  }

  new ScalaLanguage with ScalanSparkConf {

    val sparkLib_ = new ScalaLib("spark-core_2.10-1.2.0.jar", "") {}

    val mapSparkLib2Scala = {
      import sparkLib._
      import scala.language.reflectiveCalls

      scala.collection.mutable.Map()
    }

    val backend = new ScalaBackend {
      val functionMap = mapSparkLib2Scala.asInstanceOf[scala.collection.mutable.Map[Method, Func]]
    }
  }
}

