package scalan.meta

object SparkBoilerplateTool extends BoilerplateTool {
  val sparkTypeSynonims = Map(
    "RepSparkConf" -> "SSparkConf",
    "RepSparkContext" -> "SSparkContext",
    "RepRDD" -> "SRDD",
    "RepPairRDDFunctions" -> "SPairRDDFunctions",
    "RepBasePartitioner" -> "SBasePartitioner",
    "RepBroadcast" -> "SBroadcast",
    "Coll" -> "Collection",
    "PairColl" -> "IPairCollection"
  )
  lazy val sparkConfig = CodegenConfig(
    name = "ScalanSpark",
    srcPath = "src/main/scala/scalan/spark",
    entityFiles = List(
      //"SparkConf.scala",
      //"SparkContext.scala",
      "RDD.scala" //,
      //"PairRDDFunctions.scala",
      //"Partitioner.scala",
      //"Broadcast.scala"
    ),
    baseContextTrait = "ScalanCommunityDsl",
    seqContextTrait = "ScalanCommunityDslSeq",
    stagedContextTrait = "ScalanCommunityDslExp",
    extraImports = List(
      "scala.reflect._",
      "scala.reflect.runtime.universe._",
      "scalan.common.Default"),
    sparkTypeSynonims
  )

  lazy val rddCollConfig = CodegenConfig(
    name = "ScalanSpark",
    srcPath = "src/main/scala/collections",
    entityFiles = List(
      "RDDCollections.scala"
     ),
    baseContextTrait = "SparkDsl",
    seqContextTrait = "SparkDslSeq",
    stagedContextTrait = "SparkDslExp",
    extraImports = List(
      "scala.reflect._",
      "scala.reflect.runtime.universe._",
      "scalan.common.Default"),
    sparkTypeSynonims
  )

  lazy val sparkLAConfig = CodegenConfig(
    name = "ScalanSpark",
    srcPath = "src/main/scala/la",
    entityFiles = List(
      "SparkMatrices.scala"
    ),
    baseContextTrait = "SparkDsl",
    seqContextTrait = "SparkDslSeq",
    stagedContextTrait = "SparkDslExp",
    extraImports = List(
      "scala.reflect._",
      "scala.reflect.runtime.universe._",
      "scalan.common.Default",
      "scalan.spark._"
    ),
    sparkTypeSynonims ++ laTypeSynonyms
  )

  override def getConfigs(args: Array[String]) = Seq( sparkConfig/*, rddCollConfig, sparkLAConfig*/)

  override def main(args: Array[String]) = super.main(args)
}
