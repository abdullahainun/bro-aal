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
        timestamp: Timestamp,
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
    case class ClassificationObj(
        timestamp: Timestamp,
        uid: String,
        idOrigH: String,
        idOrigP: Integer,
        idRespH: String,
        idRespP: Integer,
        proto: String,
        service: String,
        duration: Double,
        origBytes: Integer,
        respBytes: Integer,
        connState: String,
        localOrig: Boolean,
        localResp: Boolean,
        missedBytes: Integer,
        history: String,
        origPkts: Integer,
        origIpBytes: Integer,
        respPkts: Integer,
        respIpBytes: Integer,
        PX: Integer,
        NNP: Integer,
        NSP: Integer,
        PSP: Double,
        IOPR: Double,
        Reconnect: Integer,
        FPS: Integer,
        TBT: Integer,
        APL: Integer,
        PPS: Double
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
      val kafkaUrl = "157.230.241.208:9092"
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

      val connSchema : StructType = StructType(
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

      val classificationSchema : StructType = StructType(
        Seq(StructField("timestamp", TimestampType, true),
          StructField("uid", IntegerType, true),
          StructField("id.orig_h", IntegerType, true),
          StructField("id.orig_p", IntegerType, true),
          StructField("id.resp_h", IntegerType, true),
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
          StructField("tunnel_parents", ArrayType(StringType, true)),
          StructField("PX", IntegerType, true),
          StructField("NNP", IntegerType, true),
          StructField("NSP", IntegerType, true),
          StructField("PSP", DoubleType, true),
          StructField("IOPR", DoubleType, true),
          StructField("Reconnect", IntegerType, true),
          StructField("FPS", IntegerType, true),
          StructField("TBT", IntegerType, true),
          StructField("APL", IntegerType, true),
          StructField("PPS", DoubleType, true)
        )
      )      

    // conn kafka stream
    val parsedLogData = kafkaStreamDF
      .select(col("value")
        .cast(StringType)
        .as("col")
      )
      .select(from_json(col("col"), connSchema)
        .getField("conn")
        .alias("conn")
      )
      .select("conn.*")           

    parsedLogData.select("*")
    .writeStream
    .outputMode("append")
    .format("console")
    .start()
 

    // convert double to timestamp
    val parsedRawDf = parsedLogData.withColumn("ts",to_utc_timestamp(
        from_unixtime(col("ts")),"GMT").alias("ts").cast(TimestampType))

    val connDf = parsedRawDf
        .map((r:Row) => ConnCountObj(r.getAs[Timestamp](0),
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
    // connDf.printSchema()    
    // classification datafame  
    // add formula column
    val PX  = connDf.withColumn("PX", BroConnFeatureExtractionFormula.px(col("origPkts").cast("int"), col("respPkts").cast("int")))
    val NNP = PX.withColumn("NNP", BroConnFeatureExtractionFormula.nnp(col("PX").cast("int")))
    val NSP = NNP.withColumn("NSP", BroConnFeatureExtractionFormula.nsp(col("PX").cast("int")))
    val PSP = NSP.withColumn("PSP", BroConnFeatureExtractionFormula.psp(col("NSP").cast("double"), col("PX").cast("double")))
    val IOPR = PSP.withColumn("IOPR", BroConnFeatureExtractionFormula.iopr(col("origPkts").cast("int"), col("respPkts").cast("int")))
    val Reconnect = IOPR.withColumn("Reconnect", lit(0))
    val FPS = Reconnect.withColumn("FPS", BroConnFeatureExtractionFormula.fps(col("origIpBytes").cast("int"), col("respPkts").cast("int")))
    val TBT = FPS.withColumn("TBT", BroConnFeatureExtractionFormula.tbt(col("origIpBytes").cast("int"), col("respIpBytes").cast("int")))
    val APL = TBT.withColumn("APL", BroConnFeatureExtractionFormula.apl(col("PX").cast("int"), col("origIpBytes").cast("int"), col("respIpBytes").cast("int")))
    val PPS = APL.withColumn("PPS", lit(0.0))

    val classificationDf = PPS  

    // classificationDf.printSchema()
// Load and parse the data
    val connModel = PipelineModel.load("hdfs://localhost:9000/user/hduser/aal/tmp/isot-dt-model")

    val assembler = new VectorAssembler()
        .setInputCols(Array(
            "idOrigP",
            "idRespP",
            "orig_bytes",
            "resp_bytes",
            "missedBytes",
            "origPkts",
            "origIpBytes",
            "respPkts",
            "respIpBytes",
            "PX",
            "NNP",
            "NSP",
            "PSP",
            "IOPR",
            "Reconnect",
            "FPS",
            "TBT",
            "APL",
            "PPS"
          ))
        .setOutputCol("features")
    val smallClassificationDf = classificationDf
      .select(
        col("idOrigP"),
        col("idRespP"),
        col("orig_bytes"),
        col("resp_bytes"),
        col("missedBytes"),
        col("origPkts"),
        col("origIpBytes"),
        col("respPkts"),
        col("respIpBytes"),
        col("PX"),
        col("NNP"),
        col("NSP"),
        col("PSP"),
        col("IOPR"),
        col("Reconnect"),
        col("FPS"),
        col("TBT"),
        col("APL"),
        col("PPS")
      )

    val filtered  = smallClassificationDf.filter(
      $"idOrigP".isNotNull &&
      $"idRespP".isNotNull &&
      $"orig_bytes".isNotNull &&
      $"resp_bytes".isNotNull &&
      $"missedBytes".isNotNull &&
      $"origPkts".isNotNull &&
      $"origIpBytes".isNotNull &&
      $"respPkts".isNotNull &&
      $"respIpBytes".isNotNull &&
      $"PX".isNotNull &&
      $"NNP".isNotNull &&
      $"NSP".isNotNull &&
      $"PSP".isNotNull &&
      $"IOPR".isNotNull &&
      $"Reconnect".isNotNull &&
      $"FPS".isNotNull &&
      $"TBT".isNotNull &&
      $"APL".isNotNull &&
      $"PPS".isNotNull
    )

    // val output = assembler.transform(filtered)
    // // output.printSchema()
    // // Make predictions on test documents.
    // val testing = connModel.transform(output)
    // testing.printSchema()    
    filtered.select("*")
    .writeStream
    .outputMode("append")
    .format("console")
    .start()
 

    spark.streams.awaitAnyTermination()
  }
}

