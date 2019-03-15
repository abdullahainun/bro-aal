package com.aal.spark.jobs

/**
  * Created by aal on 17/10/18.
  */

import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
// import org.apache.spark.sql.functions.{col, from_json, from_unixtime, to_utc_timestamp, lit}
import org.apache.spark.sql.types._
import org.bson._

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


object StreamClassification extends StreamUtils {
  case class ConnCountObj(
                           idOrigP: Integer,
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


  def main(args: Array[String]): Unit = {
    val kafkaUrl = "157.230.241.208:9092"
    //val shemaRegistryURL = "http://localhost:8081"
    val topic ="broconn"

    val spark = getSparkSession(args)
    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")
 //   spark.sparkContext.setLogLevel("INFO")

// streaming on
    val kafkaStreamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",kafkaUrl)
      .option("subscribe", topic)
      .option("startingOffsets","latest")
      .load()
    // ========== DF with no aggregations ==========
    val noAggDF = kafkaStreamDF.select("*")

    // Print new data to console
    //  noAggDF
    //   .writeStream
    //   .format("console")
    //  .start()

    val schema : StructType = StructType(
      Seq(StructField
      ("conn", StructType(Seq(
        StructField("id.orig_p", IntegerType, true),
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

    val konversi = udf((row: String) => {
      row.replaceAll("id.orig_h", "id_orig_h")
    })

    // versi ando
    val parsedLogData = kafkaStreamDF
      .select("value")
      .withColumn("col", konversi(col("value").cast("string")))
      //.select(col("value")
      //  .cast(StringType)        
      //  .as("col")
     // )
      .select(from_json(col("col"), schema)
        .getField("conn")
        .alias("conn")
      )
      .select("conn.*")

    // parsedLogData.printSchema
        // Print new data to console
    // parsedLogData
    //   .writeStream
    //   .format("console")
    //   .outputMode("append")
    //   .start()


    val calcDF = parsedLogData  
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
      // .withColumn("label", BroConnLabeling.labeling(col("id_orig_h").cast("string")))
    
    val filterDf = calcDF
      .withColumn("orig_bytes", when(col("orig_bytes").cast("integer").isNull, lit(0)))
      .withColumn("resp_bytes", when(col("resp_bytes").cast("integer").isNull, lit(0)))

    val connDf = filterDf
      .map((r:Row) => ConnCountObj(r.getAs[Integer](0),
        r.getAs[Integer](1),
        r.getAs[Integer](2),
        r.getAs[Integer](3),
        r.getAs[Integer](4),
        r.getAs[Integer](5),
        r.getAs[Integer](6),
        r.getAs[Integer](7),
        r.getAs[Integer](8),
        r.getAs[Integer](9),
        r.getAs[Integer](10),
        r.getAs[Integer](11),
        r.getAs[Double](12),
        r.getAs[Double](13),
        r.getAs[Integer](14),
        r.getAs[Integer](15),
        r.getAs[Integer](16),
        r.getAs[Integer](17),
        r.getAs[Double](18)
      ))

//  machine learning model $on
// Load and parse the data
    val connModel = PipelineModel.load("hdfs://127.0.0.1:9000/user/hduser/aal/tmp/isot-dt-model")

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

    val output = assembler.transform(connDf)
    // Make predictions on test documents.
    val testing = connModel.transform(output)
 
    testing.select("features", "predictedLabel")
    .writeStream
    .outputMode("append")
    .format("console")
    .start()

//  machine learning model $off

// Print new data to console
    //  connDf
      // .writeStream
      // .outputMode("append")
      // .format("console")
    //  .start()
    
    spark.streams.awaitAnyTermination()
  }
}

