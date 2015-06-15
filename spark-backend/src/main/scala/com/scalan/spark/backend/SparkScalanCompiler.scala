package com.scalan.spark.backend

import scalan.compilation.lms.CommunityLmsBackend
import scalan.compilation.lms.scalac.CommunityLmsCompilerScala
import scalan.spark.SparkDslExp
import scalan.{ScalanCommunityDslExp, ScalanCommunityExp}

trait SparkScalanCompiler extends SparkDslExp with ScalanSparkMethodMappingDSL with CommunityLmsCompilerScala {
  val lms = new CommunityLmsBackend
  override def graphPasses(config: CompilerConfig) = {
    val AllWrappersCleaner =
      constantPass(new GraphTransformPass("clean_wrappers", DefaultMirror, wrappersCleaner))
    val CacheAndFusion = (graph: PGraph) => new GraphTransformPass("cacheAndFusion", DefaultMirror /*new RDDCacheAndFusionMirror(graph)*/, fusionRewriter)
    Seq(AllUnpackEnabler, AllInvokeEnabler, CacheAndFusion, AllWrappersCleaner)
  }
}

