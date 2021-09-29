package io.keepcoding.spark.exercise.streaming

import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class AntennaMessage(timestamp: Timestamp, id: String, metric: String, value: Long)

trait StreamingJob {

  val spark: SparkSession

  def readFromKafka(kafkaServer: String, topic: String): DataFrame

  def parserJsonData(dataFrame: DataFrame): DataFrame

  def computeBytesByAntenna(dataFrame: DataFrame): DataFrame

  def computeBytesByApp(dataFrame: DataFrame): DataFrame

  def computeBytesByUser(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit]

  //con los argumentos:
  //xx.xxx.xxx.xx:9092 (direccion ip de la maquina de kafka )
  //devices (topic del kafka)
  //jdbc:postgresql://xx.xx.xxx.xxx:5432/postgres (ip del google sql)
  //bytes (tabla donde van a ir a dar los datos de kafka)
  //xxxxxxx (usuario de postgres)
  //xxxx (clave de postgres)
  // home/.../proyectoOlivaresAngel/src/main/resources/datosParquet (la ruta donde iran a parar las particiones en .parquet)
  def run(args: Array[String]): Unit = {
    val Array(kafkaServer, topic, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword, storagePath) = args
    println(s"Running with: ${args.toSeq}")
    //leemos los datos desde ell topic devices en la maquina con kafka
    val kafkaDF = readFromKafka(kafkaServer, topic)
    // parseamos con un esquema definido
    val antennaDF = parserJsonData(kafkaDF)
    //computamos la suma de todos los bytes por antena, app y usuario, cada uno por su cuenta
    val aggByAntennaDF = computeBytesByAntenna(antennaDF)
    val aggAppDF = computeBytesByApp(antennaDF)
    val aggUserDF = computeBytesByUser(antennaDF)
    // guardamos los datos desde el kafka en local en formato parquet a traves de spark, decir siguen con el esquema original
    val storageFuture = writeToStorage(antennaDF, storagePath)

    //guardamos en el postgres los datos de antena en modo append para cada dafaframe creado
    val aggFutureAntennaDF = writeToJdbc(aggByAntennaDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val aggFutureAppDF = writeToJdbc(aggAppDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val aggFutureUserDF = writeToJdbc(aggUserDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)

    //nos aseguramos de que los procesos ocurran y esperenando a los otros
    Await.result(Future.sequence(Seq(
      aggFutureAntennaDF,
      aggFutureAppDF,
      aggFutureUserDF,
      storageFuture)), Duration.Inf)

    spark.close()
  }

}
