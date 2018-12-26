package com.aal

/**
  * Created by aal on 17/10/18.
  */

import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, from_json, from_unixtime, to_utc_timestamp}
import org.apache.spark.sql.types._
import org.bson._

import scala.collection.JavaConverters._
import scala.collection.mutable


object BroStream extends StreamUtils {
  case class ConnCountObj(
                           timestamp: String,
                           uid: String,
                           idOrigH: String,
                           idOrigP: Integer,
                           idRespH: String,
                           idRespP: Integer,
                           proto: String,
                           transId: Integer,
                           query: String,
                           rcode: Double,
                           rcodeName: String,
                           AA: Boolean,
                           TC: Boolean,
                           RD: Boolean,
                           RA: Boolean,
                           Z:Integer,
                           answers:String,
                           TTLs:Double,
                           rejected: Boolean
                         )

  def main(args: Array[String]): Unit = {
    val kafkaUrl = "kafka:9092"
    val shemaRegistryURL = "http://localhost:8081"
    val topic ="bro"

    val spark = getSparkSession(args)
    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")
 //   spark.sparkContext.setLogLevel("INFO")
    val kafkaStreamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",kafkaUrl)
      .option("subscribe", topic)
      .option("startingOffsets","latest")
      .load()
    // ========== DF with no aggregations ==========
   val noAggDF = kafkaStreamDF.select("*")

    // Print new data to console
     noAggDF
     .writeStream
     .format("console")
     .start()

    val schema : StructType = StructType(
      Seq(StructField
      ("dns", StructType(Seq(StructField("ts",DoubleType,true),
        StructField("uid", StringType, true),
        StructField("id.orig_h", StringType, true),
        StructField("id.orig_p", IntegerType, true),
        StructField("id.resp_h", StringType, true),
        StructField("id.resp_p", IntegerType, true),
        StructField("proto", StringType, true),
        StructField("trans_id", IntegerType, true),
        StructField("query", StringType, true),
        StructField("rcode", IntegerType, true),
        StructField("rcode_name", StringType, true),
        StructField("AA", BooleanType, true),
        StructField("TC", BooleanType, true),
        StructField("RD", BooleanType, true),
        StructField("RA", BooleanType, true),
        StructField("Z", IntegerType, true),
        StructField("answers", ArrayType(StringType, true)),
        StructField("TTLs", ArrayType(IntegerType, true)),
        StructField("rejected", BooleanType, true)
      )
      )
      )
      )
    )

    val parsedLogData = kafkaStreamDF
      .select(col("value")
        .cast(StringType)
        .as("col")
      )
      .select(from_json(col("col"), schema)
        .getField("dns")
        .alias("dns")
      )

    // Print new data to console
     parsedLogData
     .writeStream
      .format("console")
     .start()

    val parsedRawDf = parsedLogData.select("dns.*").withColumn("ts",to_utc_timestamp(
      from_unixtime(col("ts")),"GMT").alias("ts").cast(StringType))
    val connDf = parsedRawDf
      .map((r:Row) => ConnCountObj(
        r.getAs[String](0),
        r.getAs[String](1),
        r.getAs[String](2),
        r.getAs[Integer](3),
        r.getAs[String](4),
        r.getAs[Integer](5),
        r.getAs[String](6),
        r.getAs[Integer](7),
        r.getAs[String](8),
        r.getAs[Integer](9),
        r.getAs[String](10),
        r.getAs[Boolean](11),
        r.getAs[Boolean](12),
        r.getAs[Boolean](13),
        r.getAs[Boolean](14),
        r.getAs[Integer](15),
        r.getAs[String](16),
        r.getAs[Double](17),
        r.getAs[Boolean](18)
      ))


    //Sink to Mongodb
    val ConnCountQuery = connDf
      .writeStream
//      .format("console")
      .outputMode("append")

      .foreach(new ForeachWriter[ConnCountObj] {

      val writeConfig: WriteConfig = WriteConfig(Map("uri" -> "mongodb://localhost/spark.bro"))
      var mongoConnector: MongoConnector = _
      var ConnCounts: mutable.ArrayBuffer[ConnCountObj] = _

      override def process(value: ConnCountObj): Unit = {
        ConnCounts.append(value)
      }

      override def close(errorOrNull: Throwable): Unit = {
        if (ConnCounts.nonEmpty) {
          mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>
            collection.insertMany(ConnCounts.map(sc => {
              var doc = new Document()
              doc.put("ts", sc.timestamp)
              doc.put("uid", sc.uid)
              doc.put("id_orig_h", sc.idOrigH)
              doc.put("id_orig_p", sc.idOrigP)
              doc.put("id_resp_h", sc.idRespH)
              doc.put("id_resp_p", sc.idRespP)
              doc.put("proto", sc.proto)
              doc.put("trans_id", sc.transId)
              doc.put("query", sc.query)
              doc.put("rcode", sc.rcode)
              doc.put("rcode_name", sc.rcodeName)
              doc.put("AA", sc.AA)
              doc.put("TC", sc.TC)
              doc.put("RD", sc.RD)
              doc.put("RA", sc.RA)
              doc.put("Z", sc.Z)
              doc.put("answers", sc.answers)
              doc.put("TTLs", sc.TTLs)
              doc.put("rejected", sc.rejected)
              doc
            }).asJava)
          })
        }
      }

      override def open(partitionId: Long, version: Long): Boolean = {
        mongoConnector = MongoConnector(writeConfig.asOptions)
        ConnCounts = new mutable.ArrayBuffer[ConnCountObj]()
        true
      }

    }).start()


    ConnCountQuery.awaitTermination()
  }
}

