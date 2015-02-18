package com.scalan.spark.backend

import com.scalan.spark.ScalanSparkMethodMapping

import scalan.compilation.lms.CommunityLmsBackend
import scalan.compilation.lms.scalac.CommunityLmsCompilerScala
import scalan.spark.SparkDslExp
import scalan.{ScalanCommunityDslExp, ScalanCommunityExp}

trait SparkScalanCompiler extends ScalanCommunityExp with SparkDslExp with ScalanCommunityDslExp with ScalanSparkMethodMapping with CommunityLmsCompilerScala {
  val lms = new CommunityLmsBackend
}

