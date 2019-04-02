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
        PPS:Double,
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
    // convert double to timestamp
    val parsedRawDf = parsedLogData.select("conn.*").withColumn("ts",to_utc_timestamp(
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

    // classification datafame  
    // add formula column
    val calcDF = parsedLogData.select("conn.*")
      .withColumn("timestamp",to_utc_timestamp(from_unixtime(col("ts")),"GMT").alias("ts").cast(TimestampType))
      .withColumn("PX", BroConnFeatureExtractionFormula.px(col("orig_pkts").cast("int"), col("resp_pkts").cast("int")))
      .withColumn("NNP", BroConnFeatureExtractionFormula.nnp(col("PX").cast("int")))
      .withColumn("NSP", BroConnFeatureExtractionFormula.nsp(col("PX").cast("int")))
      .withColumn("PSP", BroConnFeatureExtractionFormula.psp(col("NSP").cast("double"), col("PX").cast("double")))
      .withColumn("IOPR", BroConnFeatureExtractionFormula.iopr(col("orig_pkts").cast("int"), col("resp_pkts").cast("int")))
      .withColumn("Reconnect", lit(0))
      .withColumn("FPS", BroConnFeatureExtractionFormula.fps(col("orig_ip_bytes").cast("int"), col("resp_pkts").cast("int")))
      .withColumn("TBT", BroConnFeatureExtractionFormula.tbt(col("orig_ip_bytes").cast("int"), col("resp_ip_bytes").cast("int")))
      .withColumn("APL", BroConnFeatureExtractionFormula.apl(col("PX").cast("int"), col("orig_ip_bytes").cast("int"), col("resp_ip_bytes").cast("int")))
      .withColumn("PPS", lit(0.0))
    
    val classificationDf = calcDF.select("timestamp, uid")
    // calcDF.printSchema()
    // calcDF.select("*")
    classificationDf
    .writeStream
    .outputMode("append")
    .format("console")
    .start()

    // val geificationDf = calcDF
    //   .map((r:Row) => ClassificationObj(r.getAs[Timestamp](0),
    //     r.getAs[String](1),
    //     r.getAs[String](2),
    //     r.getAs[Integer](3),
    //     r.getAs[String](4),
    //     r.getAs[Integer](5),
    //     r.getAs[Integer](6),
    //     r.getAs[Integer](7),
    //     r.getAs[Integer](8),
    //     r.getAs[Integer](9),
    //     r.getAs[Integer](10),
    //     r.getAs[Integer](11),
    //     r.getAs[Integer](12),
    //     r.getAs[Integer](13),
    //     r.getAs[Integer](14),
    //     r.getAs[Integer](15),
    //     r.getAs[Double](16),
    //     r.getAs[Double](17),
    //     r.getAs[Integer](18),
    //     r.getAs[Integer](19),
    //     r.getAs[Integer](20),
    //     r.getAs[Integer](21),
    //     r.getAs[Double](22),
    //     r.getAs[String](23)
    //   ))

    // classificationDf.printSchema()
    // classificationDf.select("*")
    // .writeStream
    // .outputMode("append")
    // .format("console")
    // .start()
    //  machine learning model $on

// // Load and parse the data
//     val connModel = PipelineModel.load("hdfs://localhost:9000/user/hduser/aal/tmp/isot-dt-model")

//     val assembler = new VectorAssembler()
//         .setInputCols(Array(
//             "idOrigP",
//             "idRespP",
//             "orig_bytes",
//             "resp_bytes",
//             "missedBytes",
//             "origPkts",
//             "origIpBytes",
//             "respPkts",
//             "respIpBytes",
//             "PX",
//             "NNP",
//             "NSP",
//             "PSP",
//             "IOPR",
//             "Reconnect",
//             "FPS",
//             "TBT",
//             "APL",
//             "PPS"
//           ))
//         .setOutputCol("features")

//     val filtered  = connDf.filter(
//       $"idOrigP".isNotNull &&
//       $"idRespP".isNotNull &&
//       $"orig_bytes".isNotNull &&
//       $"resp_bytes".isNotNull &&
//       $"missedBytes".isNotNull &&
//       $"origPkts".isNotNull &&
//       $"origIpBytes".isNotNull &&
//       $"respPkts".isNotNull &&
//       $"respIpBytes".isNotNull &&
//       $"PX".isNotNull &&
//       $"NNP".isNotNull &&
//       $"NSP".isNotNull &&
//       $"PSP".isNotNull &&
//       $"IOPR".isNotNull &&
//       $"Reconnect".isNotNull &&
//       $"FPS".isNotNull &&
//       $"TBT".isNotNull &&
//       $"APL".isNotNull &&
//       $"PPS".isNotNull
//     )

//     val output = assembler.transform(filtered)
//     // Make predictions on test documents.
//     val testing = connModel.transform(output)

    spark.streams.awaitAnyTermination()
  }
}

