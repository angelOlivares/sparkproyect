package io.keepcoding.spark.exercise.batch

import java.sql.Timestamp
import java.time.OffsetDateTime
import org.apache.spark.sql.{DataFrame, SparkSession}

case class AntennaMessage(year: Int, month: Int, day: Int, hour: Int, timestamp: Timestamp, id: String, metric: String, value: Long)

trait BatchJob {

  val spark: SparkSession

  def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame

  def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def computeBytesCountByAntenna(dataFrame: DataFrame): DataFrame

  def computeBytesCountByApp(dataFrame: DataFrame): DataFrame

  def computeBytesCountByUser(dataFrame: DataFrame): DataFrame

  def computeUsersOverQuota(dataFrame: DataFrame, metadataDF: DataFrame, jdbcURI: String, user: String, password: String): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit

  //con los datos:
  //2021-09-28T16:00:00Z (hora seleccionada)
  ///home/.../proyectoOlivaresAngel/src/main/resources/datosParquet/data (origin de datos en .parquet)
  //jdbc:postgresql://xx.xxx.xxx.xxx:5432/postgres (Uri del postgres donde iran los datos y vendra los metadatos)
  //user_metadata (tabla de metadatos)
  //bytes_hourly (tabla que rellenaremos con este job)
  //user_quota_limit (otra tabla que llenaremos cruzando con los metadatos)
  //xxxxxx (usuario del postgres)
  //xxxx (clase del postgres)
  def run(args: Array[String]): Unit = {
    val Array(filterDate, storagePath, jdbcUri, jdbcMetadataTable, aggJdbcTable, aggJdbcQuotaTable, jdbcUser, jdbcPassword) = args
    println(s"Running with: ${args.toSeq}")
    //leemos datos particionados y solo los de la hora indicados. estos datos datos ya tienen un esquema definido que es el mismo que definimos en el job de streaming)
    val antennaDF = readFromStorage(storagePath, OffsetDateTime.parse(filterDate))
    //leemos la tabla del postgres con los metadatos
    val metadataDF = readUserMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    metadataDF.show()
    println("metadataDF")

    //computamos datos agregados por hora para atena, app y usuario
    val aggByAntennaDF = computeBytesCountByAntenna(antennaDF)
    aggByAntennaDF.show()
    println("aggByAntennaDF")

    val aggAppDF = computeBytesCountByApp(antennaDF)
    aggAppDF.show()
    println("aggAppDF")

    val aggUserDF = computeBytesCountByUser(antennaDF)
    aggUserDF.show()
    println("aggUserDF")

    //creamos un Dataframe agregando datos de total de bytes usados por usuario y los juntamos con los metadatos para obtener el email
    val aggUserOverQuotaDF = computeUsersOverQuota(antennaDF, metadataDF, jdbcUri, jdbcUser, jdbcPassword)
    aggUserOverQuotaDF.show()
    println("aggUserOverQuotaDF")

    //cargamos los dataframes al postgres
    writeToJdbc(aggByAntennaDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    writeToJdbc(aggAppDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    writeToJdbc(aggUserDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    writeToJdbc(aggUserOverQuotaDF, jdbcUri, aggJdbcQuotaTable, jdbcUser, jdbcPassword)

    spark.close()
  }

}
