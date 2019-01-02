package com.aal.spark.jobs

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
import com.aal.spark.utils._

import org.apache.spark.sql.functions.udf
import scala.collection.mutable.HashMap

// svm package
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils


object BroConnStream extends StreamUtils {
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
                           respIpBytes: Integer,
                           PX:Integer,
                           NNP:Integer,
                           NSP:Integer,
                           PSP:Integer,
                           IOPR:Integer,
                           Reconnect:Integer,
                           FPS:Integer,
                           TBT:Integer,
                           APL:Integer,
                           PPS:Double,
                           label:String
                         )


  def main(args: Array[String]): Unit = {
    val kafkaUrl = "10.148.0.3:9092"
    //val shemaRegistryURL = "http://localhost:8081"
    val topic ="broconn"

    val spark = getSparkSession(args)
    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")
 //   spark.sparkContext.setLogLevel("INFO")
    val kafkaStreamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",kafkaUrl)
      .option("subscribe", topic)
      .option("startingOffsets","earliest")
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
      ("conn", StructType(Seq(
        StructField("ts",DoubleType,true),
        StructField("uid", StringType, true),
        StructField("id_orig_h", StringType, true),
        StructField("id.orig_p", StringType, true),
        StructField("id.resp_h", StringType, true),
        StructField("id.resp_p", StringType, true),
        StructField("proto", StringType, true),
        StructField("service", StringType, true),
        StructField("duration", StringType, true),
        StructField("orig_bytes", IntegerType, true),
        StructField("resp_bytes", IntegerType, true),
        StructField("conn_state", StringType, true),
        StructField("local_orig", StringType, true),
        StructField("local_resp", StringType, true),
        StructField("missed_bytes", StringType, true),
        StructField("history", StringType, true),
        StructField("orig_pkts", StringType, true),
        StructField("orig_ip_bytes", StringType, true),
        StructField("resp_pkts", StringType, true),
        StructField("resp_ip_bytes", StringType, true)
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

    //  versi alfian
    // Transform data stream to Dataframe
    // val parsedLog = kafkaStreamDF.selectExpr("CAST(value AS STRING)").as[(String)]
    //   .select(from_json(col("col"), schema)
    //     .getField("conn")
    //     .alias("conn")        
    //   )
    //   .select("conn.*")

    // parsedLogData.printSchema
        // Print new data to console

    val ip_normal_app_host: HashMap[String, String] = HashMap(
        ("192.168.50.19", "dropbox" ),
        ("192.168.50.50", "avast"),
        ("192.168.50.51", "adobe_reader"),
        ("192.168.50.52", "Adobe_Software_Suite"),
        ("192.168.50.54", "Chrome"),
        ("192.168.50.55", "Firefox"),
        ("192.168.50.56", "Malwarebyte"),
        ("192.168.50.57", "WPS_office"),
        ("192.168.50.58", "Windows_update"),
        ("192.168.50.59", "bittorent"),
        ("192.168.50.60", "audacity"),
        ("192.168.50.61", "Bytefence"),
        ("192.168.50.63", "Thunderbird"),
        ("192.168.50.64", "avast"),
        ("192.168.50.65", "Skype"),
        ("192.168.50.66", "Facebook_massager"),
        ("192.168.50.67", "CCleaner"),
        ("192.168.50.68", "Windows_update"),
        ("192.168.50.69", "Hitmanpro")
    )

    val ip_botnet_app_host: HashMap[String, String] = HashMap(
        ("192.168.50.14", "zyklon"),
        ("192.168.50.15", "blue"),
        ("192.168.50.16", "liphyra"),
        ("192.168.50.17", "gaudox"),
        ("192.168.50.18", "blackout"),
        ("192.168.50.30", "citadel"),
        ("192.168.50.31", "citadel"),
        ("192.168.50.32", "black_energy"),
        ("192.168.50.34", "zeus")
    )

    val labeling = udf((ip_src: String) => {

      //  ip_src.map(row => {
      //     if((ip_botnet_app_host contains ip_src) == true){
      //         "malicious"
      //     }else if((ip_normal_app_host contains ip_src) == true){
      //         "normal"
      //     }else{
      //         "normal"
      //     }
      //   })

        var label = ""
        if((ip_botnet_app_host contains ip_src) == true){
             label  = "malicious"
        }else if((ip_normal_app_host contains ip_src) == true){
            label  = "normal"
        }else{
            label  = "normal"
        }

        label
    })  

    // rumus reconnect
    val reconnect = udf((history:String) => {
        var result = 0
        // rumus reconnect
        // println(history)
        // var temp = history.take(2)
        // if (temp == "Sr"){
        //     result = 1
        // }else{
        //     result = 0
        // }
        result.toString
    })

    val parsedRawDf = parsedLogData
      .withColumn("ts",to_utc_timestamp(from_unixtime(col("ts")),"GMT").alias("ts").cast(StringType))
      
    val newDF = parsedRawDf  
      .withColumn("PX", BroConnFeatureExtractionFormula.px(col("orig_pkts").cast("int"), col("resp_pkts").cast("int")))
      .withColumn("NNP", BroConnFeatureExtractionFormula.nnp(col("PX").cast("int")))
      .withColumn("NSP", BroConnFeatureExtractionFormula.nsp(col("PX").cast("int")))
      .withColumn("PSP", BroConnFeatureExtractionFormula.psp(col("NSP").cast("double"), col("PX").cast("double")))
      .withColumn("IOPR", BroConnFeatureExtractionFormula.iopr(col("orig_pkts").cast("int"), col("resp_pkts").cast("int")))
      .withColumn("Reconnect", reconnect(col("history").cast("string")))
      .withColumn("FPS", BroConnFeatureExtractionFormula.fps(col("orig_ip_bytes").cast("int"), col("resp_pkts").cast("int")))
      .withColumn("TBT", BroConnFeatureExtractionFormula.tbt(col("orig_ip_bytes").cast("int"), col("resp_ip_bytes").cast("int")))
      .withColumn("APL", BroConnFeatureExtractionFormula.apl(col("PX").cast("int"), col("orig_ip_bytes").cast("int"), col("resp_ip_bytes").cast("int")))
      .withColumn("PPS", BroConnFeatureExtractionFormula.pps(col("duration").cast("double"), col("orig_pkts").cast("int"), col("resp_pkts").cast("int")))
      .withColumn("label", BroConnLabeling.labeling(col("id_orig_h").cast("string")))
    
    val connDf = newDF
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
        r.getAs[Integer](19),
        r.getAs[Integer](20),
        r.getAs[Integer](21),
        r.getAs[Integer](22),
        r.getAs[Integer](23),
        r.getAs[Integer](24),
        r.getAs[Integer](25),
        r.getAs[Integer](26),
        r.getAs[Integer](27),
        r.getAs[Integer](28),
        r.getAs[Double](29),
        r.getAs[String](30)
      ))

    // Print new data to console
     newDF
     .writeStream
      .format("console")
     .start()
     .awaitTermination()

    // export to csv 
    connDf.write.format("com.databricks.spark.csv").save("connlog.csv")

    // start svm machine learning code
    // val assembler = new VectorAssembler().setInputCols(Array("ip", "status")).setOutputCol("features")
    // val df2 = assembler.transform(df).select("features")
    // val kmeans = new KMeans().setK(2).setSeed(1L)
    // val model = kmeans.fit(df2)
    // end svm machine learning code


    // //Sink to Mongodb
    // val ConnCountQuery = connDf
    //   .writeStream
    //   .format("console")
    //   .outputMode("append")

    //   .foreach(new ForeachWriter[ConnCountObj] {

    //       val writeConfig: WriteConfig = WriteConfig(Map("uri" -> "mongodb://127.0.0.1/bro.connlog"))
    //       var mongoConnector: MongoConnector = _
    //       var ConnCounts: mutable.ArrayBuffer[ConnCountObj] = _

    //       override def process(value: ConnCountObj): Unit = {
    //         ConnCounts.append(value)
    //         println(ConnCounts)
    //       }

    //       override def close(errorOrNull: Throwable): Unit = {
    //         if (ConnCounts.nonEmpty) {
    //           mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>
    //             collection.insertMany(ConnCounts.map(sc => {
    //               var doc = new Document()                  
    //               doc.put("ts", sc.timestamp)
    //               doc.put("uid", sc.uid)
    //               doc.put("id_orig_h", sc.idOrigH)
    //               doc.put("id_orig_p", sc.idOrigP)
    //               doc.put("id_resp_h", sc.idRespH)
    //               doc.put("id_resp_p", sc.idRespP)
    //               doc.put("duration", sc.duration)
    //               doc.put("history", sc.history)
    //               doc.put("orig_pkts", sc.origPkts)
    //               doc.put("orig_ip_bytes", sc.origIpBytes)
    //               doc.put("resp_bytes", sc.respPkts)
    //               doc.put("resp_ip_bytes", sc.respIpBytes)
    //               doc.put("PX", sc.PX)
    //               doc.put("NNP",sc.PX)
    //               doc.put("NSP",sc.NSP)
    //               doc.put("PSP",sc.PSP)
    //               doc.put("IOPR",sc.IOPR)
    //               doc.put("Reconnect",sc.Reconnect)
    //               doc.put("FPS",sc.FPS)
    //               doc.put("TBT",sc.TBT)
    //               doc.put("APL",sc.APL)
    //               doc.put("PPS",sc.PPS)
    //               doc.put("label",sc.label)
    //               doc
    //             }).asJava)
    //           })
    //         }
    //       }

    //       override def open(partitionId: Long, version: Long): Boolean = {
    //         mongoConnector = MongoConnector(writeConfig.asOptions)
    //         println(mongoConnector)
    //         ConnCounts = new mutable.ArrayBuffer[ConnCountObj]()
    //         true
    //       }

    // }).start()


    ConnCountQuery.awaitTermination()
  }
}

