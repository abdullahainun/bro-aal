package com.aal.spark.jobs
import com.aal.spark.utils._

import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
// $example off$

import org.apache.spark.ml.feature.{OneHotEncoderEstimator, VectorAssembler}
import org.apache.spark.ml.classification.GBTClassifier

import org.apache.spark.ml.feature.VectorSizeHint

import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.col

object StreamAnalysis extends StreamUtils {         

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
        PPS:Double
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

        // // sink to csv 
        // val parsedRawToCSV = connDf
        // .writeStream
        // .format("csv")        // can be "orc", "json", "csv", etc.
        // .option("checkpointLocation", "hdfs://master.pens.ac.id:9000/hduser/hadoop/dfs")
        // .option("path", "hdfs://master.pens.ac.id:9000/hduser/hadoop/dfs/connlog.csv")
        // .start()
        
        // parsedRawToCSV.awaitTermination()   
    }
}    