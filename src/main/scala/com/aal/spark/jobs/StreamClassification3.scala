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
                           orig_bytes: Integer,
                           resp_bytes: Integer,
                           missedBytes: Integer,
                           origPkts: Integer,
                           origIpBytes: Integer,
                           respPkts: Integer,
                           respIpBytes: Integer,
                           PX:Integer,
                           NNP:Integer,
                           NSP:Integer,
                           PSP:Double,
                           IOPR:Double,
                           Reconnect:Integer,
                           FPS:Integer,
                           TBT:Integer,
                           APL:Integer,
                           PPS:Double
                         )
   case class ResultObj(
                          timestamp: String,
                          uid: String,
                          idOrigH: String,
                          idOrigP: Integer,
                          idRespH: String,
                          idRespP: Integer,
                          predictedLabel: String
                         )
  case class DnsCountObj(
                           timestamp: Timestamp,
                           uid: String,
                           idOrigH: String,
                           idOrigP: Integer,
                           idRespH: String,
                           idRespP: Integer,
                           proto: String,
                           transId: Integer,
                           query: String,
                           rcode: Integer,
                           rcodeName: String,
                           AA: Boolean,
                           TC: Boolean,
                           RD: Boolean,
                           RA: Boolean,
                           Z:Integer,
                           answers:String,
                           TTLs:Integer,
                           rejected: Boolean
                         )

  def main(args: Array[String]): Unit = {
    val kafkaUrl = "10.8.0.2:9092"
    //val shemaRegistryURL = "http://localhost:8081"
    val topic ="broconn"

    val spark = getSparkSession(args)
    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

// streaming on
    val kafkaStreamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",kafkaUrl)
      .option("subscribe", topic)
      .option("startingOffsets","latest")
      .load()
    // ========== DF with no aggregations ==========
    val noAggDF = kafkaStreamDF.select("*")

// classification $on
    val schema : StructType = StructType(
      Seq(StructField
      ("conn", StructType(Seq(
        StructField("ts", StringType, true),
        StructField("uid", StringType, true),
        StructField("id.orig_h", StringType, true),
        StructField("id.orig_p", IntegerType, true),
        StructField("id.resp_h", StringType, true),
        StructField("id.resp_p", IntegerType, true),
        StructField("orig_bytes", IntegerType, true),
        StructField("resp_bytes", IntegerType, true),
        StructField("missed_bytes", IntegerType, true),
        StructField("orig_pkts", IntegerType, true),
        StructField("orig_ip_bytes", IntegerType, true),
        StructField("resp_pkts", IntegerType, true),
        StructField("resp_ip_bytes", IntegerType, true)
      )
      )
      )
      )
    )


    val parsedLogData = kafkaStreamDF
      .select("value")
      .select(from_json(col("col"), schema)
        .getField("conn")
        .alias("conn")
      )
      .select("conn.*")
    
    parsedLogData.select("*")
    .writeStream
    .outputMode("append")
    .format("console")
    .start()

    
//  machine learning model $on
// Load and parse the data
   

    spark.streams.awaitAnyTermination()
  }
}

