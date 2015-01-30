package com.scalan.spark.backend

import com.scalan.spark.ScalanSparkMethodMapping

import scalan.community.{ScalanCommunityDslExp, ScalanCommunityExp}
import scalan.compilation.lms.{CommunityBridge, CommunityLmsBackend, LmsCompiler}
import scalan.spark.SparkDslExp

trait SparkScalanCompiler extends ScalanCommunityExp with SparkDslExp with ScalanCommunityDslExp with LmsCompiler

trait SparkScalanBridge[A, B] extends CommunityBridge[A, B] with ScalanSparkMethodMapping {
  val lms = new CommunityLmsBackend
}
