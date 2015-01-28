package scalan.examples

import org.apache.spark._
import scalan._
import scalan.spark._

trait ConsoleDsl extends ScalanDsl {
  def print(s: Rep[String]): Rep[Unit]
  def read: Rep[String]
}

trait ReadBroadcast extends ScalanDsl with SparkDsl with ConsoleDsl {
  val appName: Rep[String] = "R/W Broadcast"
  val master: Rep[String] = "local"

  def startSpark: Rep[SparkContext] = {
    val conf: Rep[SparkConf] = SSparkConf().setAppName(appName).setMaster(master)

    SSparkContext(conf)
  }

  def rwDouble(sc: Rep[SparkContext], d: Rep[Double]): Rep[Double] = {
    val broadcastVar = sc.broadcast(d)

    broadcastVar.value
  }

  def main(): Rep[Unit] = {
    val pi: Rep[Double] = 3.14159
    val sparkContext = startSpark
    val bPi = rwDouble(sparkContext, pi)

    IF (pi === bPi) THEN {
      print("OK")
    } ELSE {
      print("FAIL")
    }
  }
}
