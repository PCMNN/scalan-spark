package scalan.meta

object SparkBoilerplateTool extends BoilerplateTool {
  val sparkTypeSynonims = Map(
    "RepSparkConf" -> "SparkConf",
    "RepSparkContext" -> "SparkContext",
    "RepRDD" -> "RDD",
    "RepPairRDDFunctions" -> "PairRDDFunctions",
    "RepPartitioner" -> "Partitioner",
    "RepBroadcast" -> "Broadcast"
  )
  lazy val sparkConfig = CodegenConfig(
    srcPath = "src/main/scala/scalan/spark",
    entityFiles = List(
      "SparkConf.scala",
      "SparkContext.scala",
      "RDD.scala",
      "PairRDDFunctions.scala",
      "Partitioner.scala",
      "Broadcast.scala"
    ),
    seqContextTrait = "ScalanSeq",
    stagedContextTrait = "ScalanExp",
    extraImports = List(
      "scala.reflect.runtime.universe._",
      "scalan.common.Default"),
    sparkTypeSynonims
  )

  val sparkArraysTypeSynonims = Map[String, String]()
  lazy val sparkArraysConfig = CodegenConfig(
    srcPath = "src/main/scala/scalan/spark/parrays",
    entityFiles = List(
      "SparkArrays.scala"
    ),
    seqContextTrait = "ScalanSeq",
    stagedContextTrait = "ScalanExp",
    extraImports = List(
      "scala.reflect.runtime.universe._",
      "scalan.common.Default"),
    sparkArraysTypeSynonims
  )

  override def getConfigs(args: Array[String]) = Seq(sparkConfig, sparkArraysConfig)

  override def main(args: Array[String]) = super.main(args)
}
