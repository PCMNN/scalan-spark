package scalan.meta

object SparkBoilerplateTool extends BoilerplateTool {
  val sparkTypeSynonims = Map(
    "RepSparkConf" -> "SparkConf",
    "RepSparkContext" -> "SparkContext",
    "RepRDD" -> "RDD",
    "RepPairRDD" -> "PairRDDFunctions",
    "RepBroadcast" -> "Broadcast"
  )
  lazy val sparkConfig = CodegenConfig(
    srcPath = "src/main/scala/scalan/spark",
    entityFiles = List(
      "SparkConf.scala",
      "SparkContext.scala",
      "RDD.scala",
      "PairRDD.scala",
      "Broadcast.scala"
    ),
    seqContextTrait = "ScalanSeq",
    stagedContextTrait = "ScalanExp",
    extraImports = List(
      "scala.reflect.runtime.universe._",
      "scalan.common.Default"),
    sparkTypeSynonims
  )

  override def getConfigs(args: Array[String]) = Seq(sparkConfig)

  override def main(args: Array[String]) = super.main(args)
}
