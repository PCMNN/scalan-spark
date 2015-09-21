package com.scalan.spark.backend

import scalan.collections.SeqsScalaMethodMapping
import scalan.compilation.lms.{CommunityLmsBackendBase, CommunityLmsBackend}
import scalan.compilation.lms.scalac.{ScalaCommunityCodegen, CommunityLmsCompilerScala}
import scalan.spark.SparkDslExp
import scalan.{ScalanCommunityDslExp, ScalanCommunityExp}

trait SparkLmsBackendBase extends CommunityLmsBackendBase with SparkArrayOpsExp

class SparkLmsBackend extends CommunityLmsBackend with SparkLmsBackendBase { self =>
  override val codegen = new SparkCodegen[self.type](self)
}
class SparkCodegen[BackendCake <: SparkLmsBackendBase](backend: BackendCake)
  extends ScalaCommunityCodegen[BackendCake](backend) with SparkScalaGenArrayOps

class SparkScalanCompiler[ScalanCake <: SparkDslExp](_scalan: ScalanCake)
  extends CommunityLmsCompilerScala[ScalanCake](_scalan) with SparkLmsBridge {
  import scalan._

  override val lms = new SparkLmsBackend //CommunityLmsBackend
  val AllWrappersCleaner =
    constantPass(new GraphTransformPass("clean_wrappers", DefaultMirror, wrappersCleaner))
  val CacheAndFusion = (graph: PGraph) => new GraphTransformPass("cacheAndFusion", DefaultMirror /*new RDDCacheAndFusionMirror(graph)*/, fusionRewriter)
  override def graphPasses(config: CompilerConfig) = {
    Seq(AllUnpackEnabler, AllInvokeEnabler, /*CacheAndFusion,*/ AllWrappersCleaner)
  }
}

