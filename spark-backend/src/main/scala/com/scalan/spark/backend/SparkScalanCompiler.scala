package com.scalan.spark.backend

import scalan.collections.SeqsScalaMethodMapping
import scalan.compilation.lms.{CommunityLmsBackendBase, CommunityLmsBackend}
import scalan.compilation.lms.scalac.{ScalaCommunityCodegen, CommunityLmsCompilerScala}
import scalan.spark.SparkDslExp
import scalan.{ScalanCommunityDslExp, ScalanCommunityExp}

trait SparkLmsBackendBase extends CommunityLmsBackendBase with SparkArrayOpsExp

class SparkLmsBackend extends SparkLmsBackendBase { self =>
  override val codegen = new SparkCodegen[self.type](self)
}
class SparkCodegen[BackendCake <: SparkLmsBackendBase](backend: BackendCake)
  extends ScalaCommunityCodegen[BackendCake](backend) with SparkScalaGenArrayOps

trait SparkScalanCompiler extends SparkDslExp with ScalanSparkMethodMappingDSL with CommunityLmsCompilerScala with SparkLmsBridge {
  val lms = new SparkLmsBackend //CommunityLmsBackend
  override def graphPasses(config: CompilerConfig) = {
    val AllWrappersCleaner =
      constantPass(new GraphTransformPass("clean_wrappers", DefaultMirror, wrappersCleaner))
    val CacheAndFusion = (graph: PGraph) => new GraphTransformPass("cacheAndFusion", DefaultMirror /*new RDDCacheAndFusionMirror(graph)*/, fusionRewriter)
    Seq(AllUnpackEnabler, AllInvokeEnabler, /*CacheAndFusion,*/ AllWrappersCleaner)
  }
}

