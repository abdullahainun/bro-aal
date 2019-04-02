package com.aal.spark.jobs

/**
  * Created by aal on 17/10/18.
  */

import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, from_json, from_unixtime, to_utc_timestamp, lit, to_timestamp}
import org.apache.spark.sql.types._
import org.bson._
import java.sql.Timestamp

import scala.collection.JavaConverters._
import scala.collection.mutable
import com.aal.spark.utils._

import org.apache.spark.sql.functions.udf
import scala.collection.mutable.HashMap

// pipeline package
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType


object StreamClassification3 extends StreamUtils {  
        case class ConnCountObj(
                   timestamp: String,
                   uid: String,
                   idOrigH: String,
                   idOrigP: Integer,
                   idRespH: String,
                   idRespP: Integer,
                   proto: String,
                   service: String,
                   duration: Double,
                   orig_bytes: Integer,
                   resp_bytes: Integer,
                   connState: String,
                   localOrig: Boolean,
                   localResp: Boolean,
                   missedBytes: Integer,
                   history: String,
                   origPkts: Integer,
                   origIpBytes: Integer,
                   respPkts: Integer,
                   respIpBytes: Integer
                   )

    def main(args: Array[String]): Unit = {
      val kafkaUrl = "localhost:9092"      
      val topic ="broconn"

      val spark = getSparkSession(args)
      import spark.implicits._

      spark.sparkContext.setLogLevel("ERROR")
      val kafkaStreamDF = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",kafkaUrl)
        .option("subscribe", topic)
        .option("startingOffsets","latest")
        .load()

      val schema : StructType = StructType(
        Seq(StructField
        ("conn", StructType(Seq(
          StructField("ts",DoubleType,true),
          StructField("uid", StringType, true),
          StructField("id.orig_h", StringType, true),
          StructField("id.orig_p", IntegerType, true),
          StructField("id.resp_h", StringType, true),
          StructField("id.resp_p", IntegerType, true),
          StructField("proto", StringType, true),
          StructField("service", StringType, true),
          StructField("duration", DoubleType, true),
          StructField("orig_bytes", IntegerType, true),
          StructField("resp_bytes", IntegerType, true),
          StructField("conn_state", StringType, true),
          StructField("local_orig", BooleanType, true),
          StructField("local_resp", BooleanType, true),
          StructField("missed_bytes", IntegerType, true),
          StructField("history", StringType, true),
          StructField("orig_pkts", IntegerType, true),
          StructField("orig_ip_bytes", IntegerType, true),
          StructField("resp_pkts", IntegerType, true),
          StructField("resp_ip_bytes", IntegerType, true),
          StructField("tunnel_parents", ArrayType(StringType, true)))
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
          .getField("conn")
          .alias("conn")
        )

      val parsedRawDf = parsedLogData.select("conn.*").withColumn("ts",to_utc_timestamp(
        from_unixtime(col("ts")),"GMT").alias("ts").cast(StringType))

      val connDf = parsedRawDf
        .map((r:Row) => ConnCountObj(r.getAs[String](0),
          r.getAs[String](1),
          r.getAs[String](2),
          r.getAs[Integer](3),
          r.getAs[String](4),
          r.getAs[Integer](5),
          r.getAs[String](6),
          r.getAs[String](7),
          r.getAs[Double](8),
          r.getAs[Integer](9),
          r.getAs[Integer](10),
          r.getAs[String](11),
          r.getAs[Boolean](12),
          r.getAs[Boolean](13),
          r.getAs[Integer](14),
          r.getAs[String](15),
          r.getAs[Integer](16),
          r.getAs[Integer](17),
          r.getAs[Integer](18),
          r.getAs[Integer](19)
        ))

    connDf.select("*")
    .writeStream
    .outputMode("append")
    .format("console")
    .start()

    //Sink to Mongodb
      val ConnCountQuery = connDf
          .writeStream
//        .format("console")
//        .option("truncate", "false")
          .outputMode("append")
//        .start()
//        .awaitTermination()

        .foreach(new ForeachWriter[ConnCountObj] {

          val writeConfig: WriteConfig = WriteConfig(Map("uri" -> "mongodb://admin:jarkoM@10.8.0.2:27017/aal.conn?replicaSet=rs0&authSource=admin"))
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
                  doc.put("service", sc.service)
                  doc.put("duration", sc.duration)
                  doc.put("orig_bytes", sc.orig_bytes)
                  doc.put("conn_state", sc.connState)
                  doc.put("local_orig", sc.localOrig)
                  doc.put("local_resp", sc.localResp)
                  doc.put("missed_bytes", sc.missedBytes)
                  doc.put("history", sc.history)
                  doc.put("orig_pkts", sc.origPkts)
                  doc.put("orig_ip_bytes", sc.origIpBytes)
                  doc.put("resp_bytes", sc.respPkts)
                  doc.put("resp_ip_bytes", sc.respIpBytes)
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
    spark.streams.awaitAnyTermination()
  }
}

