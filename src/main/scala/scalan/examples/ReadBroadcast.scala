package scalan.examples

import scalan._
import scalan.spark._

trait ReadBroadcast extends ScalanDsl with SparkDsl {
  val appName: Rep[String] = "R/W Broadcast"
  val master: Rep[String] = "local"

  def startSpark: SSparkContext = {
    val conf = SSparkConf().setAppName(appName).setMaster(master)

    SSparkContext(conf)
  }

  def rwDouble(sc: SSparkContext, d: Rep[Double]): Rep[Double] = {
    val broadcastVar = sc.broadcast(d)

    broadcastVar.value
  }

  def main(): Unit = {
    val pi: Rep[Double] = 3.14159
    val sparkContext = startSpark
    val bPi = rwDouble(sparkContext, pi)

    IF (pi === bPi) THEN {
      println("OK")
    } ELSE {
      println("FAIL")
    }
  }
}
