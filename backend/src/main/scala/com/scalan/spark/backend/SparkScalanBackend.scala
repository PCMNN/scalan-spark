package com.scalan.spark.backend

import com.scalan.spark.ScalanSparkMethodMapping

import scalan.community.{ScalanCommunityDslExp, ScalanCommunityExp}
import scalan.compilation.lms.scalac.LmsCompilerScala
import scalan.compilation.lms.{CommunityBridge, CommunityLmsBackend, LmsCompiler}
import scalan.spark.SparkDslExp

trait SparkScalanCompiler extends ScalanCommunityExp with SparkDslExp with ScalanCommunityDslExp with LmsCompilerScala with ScalanSparkMethodMapping with CommunityBridge {
  val lms = new CommunityLmsBackend
}

