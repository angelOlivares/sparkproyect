package io.keepcoding.spark.exercise.batch

import java.time.OffsetDateTime
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object BtcJobImp extends BatchJob {
  def main(args: Array[String]): Unit = run(args)

  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .option("path", storagePath)
      .load()
      .filter(
        $"year" === filterDate.getYear &&
          $"month" === filterDate.getMonthValue &&
          $"day" === filterDate.getDayOfMonth &&
          $"hour" === filterDate.getHour
      )
      .persist()
  }

  override def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {

    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  override def computeBytesCountByAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"antenna_id".as("id"), $"bytes", $"timestamp")
      .groupBy($"id", window($"timestamp", "1 hour") )
      .agg(
        sum($"bytes").as("value"))
      .withColumn("type", lit("antenna_total_bytes"))
      .withColumn("timestamp", $"window.start")
      .select($"timestamp", $"id", $"value", $"type")
  }

  override def computeBytesCountByUser(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select ($"bytes", $"id".as("id"), $"timestamp")
      .groupBy($"id", window($"timestamp", "1 hour") )
      .agg(
        sum($"bytes").as("value"))
      .withColumn("type", lit("user_total_bytes"))
      .withColumn("timestamp", $"window.start")
      .select($"timestamp", $"id", $"value", $"type")
  }

  override def computeBytesCountByApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"app".as("id"), $"bytes", $"timestamp")
      .groupBy($"id", window($"timestamp", "1 hour") )
      .agg(
        sum($"bytes").as("value"))
      .withColumn("type", lit("app_total_bytes"))
      .withColumn("timestamp", $"window.start")
      .select($"timestamp", $"id", $"value", $"type")
  }

  override def computeUsersOverQuota(dataFrame: DataFrame, metadataDF: DataFrame, jdbcURI: String, user: String, password: String): DataFrame = {
    dataFrame
      .as("a")
      .select($"id",$"bytes",$"timestamp")
      .groupBy($"id", window($"timestamp", "1 hour") )
      .agg(
        sum($"bytes").as("value"))
      .withColumn("timestamp", $"window.start")
      .select($"timestamp", $"id", $"value")
      .join(
      metadataDF.select($"id", $"email", $"quota").as("b"),
      $"a.id" === $"b.id" && $"value" > $"b.quota")
      .select($"b.email", $"value".as("usage"), $"b.quota", $"timestamp")
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()
  }
}
