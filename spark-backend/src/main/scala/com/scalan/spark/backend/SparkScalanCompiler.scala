package com.scalan.spark.backend

import scalan.compilation.lms.CommunityLmsBackend
import scalan.compilation.lms.scalac.CommunityLmsCompilerScala
import scalan.spark.SparkDslExp
import scalan.{ScalanCommunityDslExp, ScalanCommunityExp}

trait SparkScalanCompiler extends ScalanCommunityExp with SparkDslExp with ScalanCommunityDslExp with ScalanSparkMethodMappingDSL with CommunityLmsCompilerScala {
  val lms = new CommunityLmsBackend
}

