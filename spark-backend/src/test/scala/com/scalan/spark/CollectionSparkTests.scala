package com.scalan.spark

import com.scalan.spark.backend.SparkScalanCompiler
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.scalatest.BeforeAndAfterAll

import scalan.it.ItTestsUtil
import scalan.BaseTests
import la.SparkLADslExp

class CollectionSparkTests extends BaseTests with BeforeAndAfterAll with ItTestsUtil { suite =>
  trait CollectionSamplesExp extends SparkLADslExp {
    lazy val zipRddVector = fun { rdd: Rep[RDD[(Long, Array[Int])]] =>
      val res = RDDIndexedCollection(SRDDImpl(rdd))
      res.map(rdd => rdd.map { p: Rep[Int] => p + 1 }).arr
    }
  }
  class ProgramExp extends CollectionSamplesExp {
    val sparkContext: SparkContext = null
    val sSparkContext = ExpSSparkContextImpl(sparkContext)
    val repSparkContext = SSparkContext(SSparkConf())
  }
  class TestCompiler[ScalanCake <: CollectionSamplesExp](program: ScalanCake) extends SparkScalanCompiler(program)
  def generationConfig[ScalanCake <: SparkLADslExp](cmpl: SparkScalanCompiler[ScalanCake], pack : String = "gen", command: String = null, getOutput: Boolean = false) =
    cmpl.defaultCompilerConfig.copy(scalaVersion = Some("2.10.4"),
      sbt = cmpl.defaultCompilerConfig.sbt.copy(mainPack = Some(s"com.scalan.spark.$pack"),
        resources = Seq("log4j.properties"),
        extraClasses = Seq("com.scalan.spark.method.Methods"),
        toSystemOut = !getOutput,
        commands = if (command == null) cmpl.defaultCompilerConfig.sbt.commands else Seq(command)))

  ignore("Collection/RDD Code Gen") { // Don't know how to mirror ExpSRDDImpl
    val testCompiler = new TestCompiler(new ProgramExp)
    val compiled = compileSource(testCompiler)(testCompiler.scalan.zipRddVector, "zipRddVector", generationConfig(testCompiler, "zipRddVector", "package"))
  }
}