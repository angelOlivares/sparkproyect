package io.keepcoding.spark.exercise.streaming
//importamos paquetes necesarios
import org.apache.spark.sql.functions.{ col, dayofmonth, from_json, hour, month, lit, sum, window, year}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

//extendemos las funciones que hay en StreamingJob
object StmJobImp extends StreamingJob {
  def main(args: Array[String]): Unit = run(args)

  //Creamos la Session de Spark en local
  override val spark: SparkSession =
    SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()
  }
  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    val jsonSchema = StructType(Seq(
      StructField("bytes", LongType, nullable = false),
      StructField("timestamp", TimestampType, nullable = false),
      StructField("app", StringType, nullable = false),
      StructField("id", StringType, nullable = false),
      StructField("antenna_id", StringType, nullable = false),
    ))

    dataFrame
      .select(from_json(col("value").cast(StringType), jsonSchema).as("json"))
      .select("json.*")
  }


  import spark.implicits._

  override def computeBytesByAntenna(dataFrame: DataFrame): DataFrame =  {

    dataFrame
      .select($"timestamp".cast(TimestampType).as("timestamp"), $"antenna_id", $"bytes")
      .withWatermark("timestamp", "1 minutes")
      .groupBy(window($"timestamp", "5 minutes"), $"antenna_id")
      .agg(sum($"bytes").as("total_bytes"))
      .withColumn("type", lit("antenna_total_bytes"))
      .select(
        $"window.start".cast(TimestampType).as("timestamp"),
        $"antenna_id".as("id"),
        $"total_bytes".as("value"),
        $"type".as("type")
      )
  }

  override def computeBytesByApp(dataFrame: DataFrame): DataFrame =  {

    dataFrame
      .select($"timestamp".cast(TimestampType).as("timestamp"), $"app", $"bytes")
      .withWatermark("timestamp", "1 minutes")
      .groupBy(window($"timestamp", "5 minutes"), $"app")
      .agg(sum($"bytes").as("total_bytes"))
      .withColumn("type", lit("app_total_bytes"))
      .select(
        $"window.start".cast(TimestampType).as("timestamp"),
        $"app".as("id"),
        $"total_bytes".as("value"),
        $"type".as("type")
      )

  }

  override def computeBytesByUser(dataFrame: DataFrame): DataFrame =  {

    dataFrame
      .select($"timestamp".cast(TimestampType).as("timestamp"), $"id", $"bytes")
      .withWatermark("timestamp", "30 seconds")
      .groupBy(window($"timestamp", "1 minutes"), $"id")
      .agg(sum($"bytes").as("total_bytes"))
      .withColumn("type", lit("user_total_bytes"))
      .select(
        $"window.start".cast(TimestampType).as("timestamp"),
        $"id".as("id"),
        $"total_bytes".as("value"),
        $"type".as("type")
      )
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] =
    Future {
      dataFrame
        .writeStream
        .foreachBatch { (batch: DataFrame, long: Long) =>
          batch
            .write
            .mode(SaveMode.Append)
            .format("jdbc")
            //.option("driver", "org.postgresql.Driver")
            .option("url", jdbcURI)
            .option("dbtable", jdbcTable)
            .option("user", user)
            .option("password", password)
            .save()
        }
        .start()
        .awaitTermination()
    }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {

    val columns = dataFrame.columns.map(col).toSeq ++
      Seq(
        year($"timestamp").as("year"),
        month($"timestamp").as("month"),
        dayofmonth($"timestamp").as("day"),
        hour($"timestamp").as("hour")
      )

    dataFrame
      .select(columns: _*)
      .writeStream
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .option("path",  s"${storageRootPath}/data")
      .option("checkpointLocation", s"${storageRootPath}/checkpoint")
      .start()
      .awaitTermination()

  }


}

