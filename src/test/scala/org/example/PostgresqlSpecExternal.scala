package org.example

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

import java.time.Instant


class PostgresqlSpecExternal extends AnyFlatSpec  {

  val partitionSize = 10
  val partitionsNum = 5
  val connection = Map(
    "url" -> "jdbc:postgresql://localhost:5432/postgres",
    "user" -> "docker",
    "password" -> "docker"
  )
  val readFromTable = "public.users_source"
  val writeToTable = "public.users_destination"

  "PostgreSQL data source" should "read table" in {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("PostgresReaderJob")
      .getOrCreate()

    val df = spark.read
      .format("org.example.datasource.postgres")
      .options(connection)
      .option("tableName", readFromTable)
      .option("partitionSize", partitionSize)
      .load()

    assert(df.rdd.partitions.length == partitionsNum)

    df.show()

    spark.stop()
  }

  "PostgreSQL data source" should "write table" in {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("PostgresWriterJob")
      .getOrCreate()

    import spark.implicits._

    val x = Instant.now.getEpochSecond
    val df = (x to x + 10).map(_.toLong).toDF("user_id")

    df
      .write
      .format("org.example.datasource.postgres")
      .options(connection)
      .option("tableName", writeToTable)
      .option("partitionSize", partitionSize)
      .mode(SaveMode.Append)
      .save()

    spark.stop()
  }

}
