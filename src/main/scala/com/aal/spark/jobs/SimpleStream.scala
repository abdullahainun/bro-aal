package com.aal.spark.jobs

import java.time.{LocalDate, Period}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType}

object SimpleStream{
  def main(args: Array[String]): Unit = {
    new StreamsProcessor("localhost:9092").process()
  }
}

class SimpleStream(brokers: String) {

  def process(): Unit = {

    val spark = SparkSession.builder()
      .appName("kafka-tutorials")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val inputDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", "sample")
      .load()

    val personJsonDf = inputDf.selectExpr("CAST(value AS STRING)")

    val struct = new StructType()
      .add("firstName", DataTypes.StringType)
      .add("lastName", DataTypes.StringType)
    val personNestedDf = personJsonDf.select(from_json($"value", struct).as("person"))

    val personFlattenedDf = personNestedDf.selectExpr("person.firstName", "person.lastName")

   val consoleOutput = processedDf.writeStream
     .outputMode("append")
     .format("console")
     .start()

    spark.streams.awaitAnyTermination()
  }

}