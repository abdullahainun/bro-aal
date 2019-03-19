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
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType


object StreamClassification extends StreamUtils {
  case class ConnCountObj(
                           uid: String,
                           idOrigH: String,
                           idRespH: String,
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
   case class ResultObj(
                          uid: String,
                          idOrigH: String,
                          idOrigP: Integer,
                          idRespH: String,
                          idRespP: Integer,
                          predictedLabel: String
                         )


  def main(args: Array[String]): Unit = {
    val kafkaUrl = "157.230.241.208:9092"
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

    val schema : StructType = StructType(
      Seq(StructField
      ("conn", StructType(Seq(
        StructField("uid", StringType, true),
        StructField("id.orig_h", StringType, true),
        StructField("id.resp_h", StringType, true),
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

 
    val konversi_orig_h = udf((row: String) => {
      row.replaceAll("id.orig_h", "id_orig_h")
    })
    val konversi_resp_h = udf((row: String) => {
      row.replaceAll("id.resp_h", "id_resp_h")
    })

    val parsedLogData = kafkaStreamDF
      .select("value")
      .withColumn("col", konversi_orig_h(col("value").cast("string")))
      .withColumn("col", konversi_resp_h(col("value").cast("string")))
      .select(from_json(col("col"), schema)
        .getField("conn")
        .alias("conn")
      )
      .select("conn.*")

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

    
    val connDf = calcDF
      .map((r:Row) => ConnCountObj(
        r.getAs[String](0),
        r.getAs[String](1),
        r.getAs[String](2),
        r.getAs[Integer](3),
        r.getAs[Integer](4),
        r.getAs[Integer](5),
        r.getAs[Integer](6),
        r.getAs[Integer](7),
        r.getAs[Integer](8),
        r.getAs[Integer](9),
        r.getAs[Integer](10),
        r.getAs[Integer](11),
        r.getAs[Integer](12),
        r.getAs[Integer](13),
        r.getAs[Integer](14),
        r.getAs[Double](15),
        r.getAs[Double](16),
        r.getAs[Integer](17),
        r.getAs[Integer](18),
        r.getAs[Integer](19),
        r.getAs[Integer](20),
        r.getAs[Double](21)
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

    val filtered  = connDf.filter(
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

    val output = assembler.transform(filtered)
    // Make predictions on test documents.
    val testing = connModel.transform(output)

    val malware = testing.filter($"predictedLabel".contains("1.0"))
    testing.printSchema()
    val testing2 = testing
                    .select(
                      col("uid"),
                      col("idOrigH"),
                      col("idOrigP"),
                      col("idRespH"),
                      col("idRespP"),
                      col("predictedLabel")
                    )
    testing2.printSchema()    
    testing2.select("*")
    .writeStream
    .outputMode("append")
    .format("console")
    .start()

    val resultDf = testing2
      .map((r:Row) => ResultObj(
        r.getAs[String](0),
        r.getAs[String](1),
        r.getAs[Integer](2),
        r.getAs[String](3),
        r.getAs[Integer](4),
        r.getAs[String](5),
      ))    

    // testing.select("*")
    // .writeStream
    // .outputMode("append")
    // .format("console")
    // .start()

//  machine learning model $off
// Sink to Mongodb
      val ConnCountQuery = resultDf
          .writeStream
          .format("console")
//        .option("truncate", "false")
          .outputMode("append")
//        .start()
//        .awaitTermination()

        .foreach(new ForeachWriter[ResultObj] {

          val writeConfig: WriteConfig = WriteConfig(Map("uri" -> "mongodb://admin:jarkoM@157.230.241.208:27017/aal.classification?replicaSet=rs0&authSource=admin"))
          var mongoConnector: MongoConnector = _
          var ConnCounts: mutable.ArrayBuffer[ResultObj] = _

          override def process(value: ResultObj): Unit = {
            ConnCounts.append(value)
          }

          override def close(errorOrNull: Throwable): Unit = {
            if (ConnCounts.nonEmpty) {
              mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>
                collection.insertMany(ConnCounts.map(sc => {
                  var doc = new Document()
                  doc.put("uid", sc.uid)
                  doc.put("orig_h", sc.idOrigH)
                  doc.put("orig_p", sc.idOrigP)
                  doc.put("resp_h", sc.idRespH)
                  doc.put("resp_p", sc.idRespP)
                  doc.put("label", sc.predictedLabel)
                  doc
                }).asJava)
              })
            }
          }

          override def open(partitionId: Long, version: Long): Boolean = {
            mongoConnector = MongoConnector(writeConfig.asOptions)
            ConnCounts = new mutable.ArrayBuffer[ResultObj]()
            true
          }

        }).start()

// Print new data to console
//      connDf
//       .writeStream
//       .outputMode("append")
//       .format("console")
//      .start()
    // ConnCountQuery.awaitTermination()
    spark.streams.awaitAnyTermination()
  }
}

