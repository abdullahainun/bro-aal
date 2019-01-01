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
                           NSP:Integer
                          //  PSP:Integer,
                          //  IOPR:Integer,
                          //  Reconnect:Integer,
                          //  FPS:Integer,
                          //  TBT:Integer,
                          //  APL:Integer,
                          //  PPS:Double
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
        StructField("resp_ip_bytes", IntegerType, true)
      )
      )
      )
      )
    )

    // versi ando
    val parsedLogData = kafkaStreamDF
      .select(col("value")
        .cast(StringType)
        .as("col")
      )
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

    val parsedRawDf = parsedLogData
      .withColumn("ts",to_utc_timestamp(from_unixtime(col("ts")),"GMT").alias("ts").cast(StringType))
      
    val px = udf((origPkts: Int, respPkts: Int) => {
        var result = 0
        result = origPkts + respPkts 

        result
    })

    // rumus nnp
    val nnp = udf((px: Int) => {
        var result = 0
        if( px == 0 ){
          result =  1;
        }else{
          result =  0;
        }

        result
    })

    // rumus nsp
    val nsp = udf((px: Int) => {
        var result = 0

        if(px >= 63 && px <= 400 ){
            result = 1;
        }else{
            result = 0;
        }
        result
    })

    // // rumus psp
    // val psp = udf((nsp:Double, px: Double) => {
    //     var result = px

    //     if(!(px == 0.0)){
    //         result = nsp/px
    //     }
    // })

    // // rumus iopr
    // val iopr = udf((origPkts:Int, respPkts:Int) => {
    //     var result = 0
    //     if(respPkts != 0){
    //         result = origPkts / respPkts;
    //     }else{
    //         result = 0
    //     }
    //     result
    // })

    // // rumus reconnect
    // val reconnect = udf((history:String) => {
    //     var result = 0
    //     // rumus reconnect
    //     var temp = history  contains "Sr" 
    //     if (temp == true){
    //         result = 1
    //     }else{
    //         result = 0
    //     }
    //     result
    // })

    // // rumus fps
    // val fps = udf((origIpBytes:Int, origPkts:Int) => {
    //     var result = 0
    //     if(origPkts !=0 ){
    //       result = origIpBytes / origPkts                    
    //     }else{
    //       result = 0
    //     }

    //     result
    // })

    // // rumus tbt
    // val tbt = udf((origIpBytes:Int, respIpBytes:Int) => origIpBytes + respIpBytes )

    // val apl = udf((px:Int, origIpBytes:Int, respIpBytes:Int) => {
    //     var result = 0
    //     if(px == 0){
    //         result = 0             
    //     }else{
    //         result = (origIpBytes + respIpBytes )/px
    //     }
    //     result
    // })
    // // val dpl = udf(() => {
    // //     var result = 0

    // // })
    // // val pv =  udf(() => {
    // //     var result = 0
    // // }
    // // val bs = udf(() => {
    // //     var result = 0

    // // })
    // // val ps = udf(() => {
    // //     var result = 0

    // // })
    // // val ait = udf(() => {
    // //     var result = 0

    // // })
    // val pps = udf((duration:Double, origPkts:Int, respPkts:Int) => {
    //     var result = 0.0
    //     if(px != 0){
    //         result = (origPkts + respPkts ) / duration
    //     }else{
    //         result = 0.0  
    //     }
    //     result
    // })

    val newDF = parsedRawDf  
      .withColumn("PX", px(col("orig_pkts").cast("int"), col("resp_pkts").cast("int")))
      .withColumn("NNP", nnp(col("PX").cast("int")))
      .withColumn("NSP", nsp(col("PX").cast("int")))
      // .withColumn("PSP", psp(col("NSP").cast("double"), col("PX").cast("double")))
      // .withColumn("IOPR", iopr(col("orig_pkts").cast("int"), col("resp_pkts").cast("int")))
      // .withColumn("Reconnect", reconnect(col("history").cast("string")))
      // .withColumn("FPS", px(col("orig_ip_bytes").cast("int"), col("resp_pkts").cast("int")))
      // .withColumn("TBT", px(col("orig_ip_bytes").cast("int"), col("resp_ip_bytes").cast("int")))
      // .withColumn("APL", px(col("PX").cast("int"), col("orig_ip_bytes").cast("int"), col("resp_ip_bytes").cast("int")))
      // .withColumn("PPS", px(col("duration").cast("double"), col("orig_pkts").cast("int"), col("resp_pkts").cast("int")))
    
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
        r.getAs[Integer](22)
        // r.getAs[Integer](23),
        // r.getAs[Integer](24),
        // r.getAs[Integer](25),
        // r.getAs[Integer](26),
        // r.getAs[Integer](27),
        // r.getAs[Integer](28),
        // r.getAs[Double](29)
      ))

    // Print new data to console
     newDF
     .writeStream
      .format("console")
     .start()

    // //Sink to Mongodb
    // val ConnCountQuery = connDf
    //   .writeStream
    //   .format("console")
    //   .outputMode("append")

    //   .foreach(new ForeachWriter[ConnCountObj] {

    //       val writeConfig: WriteConfig = WriteConfig(Map("uri" -> "mongodb://10.148.0.4/bro.connlog"))
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
    //               // doc.put("PSP",sc.PSP)
    //               // doc.put("IOPR",sc.IOPR)
    //               // doc.put("Reconnect",sc.Reconnect)
    //               // doc.put("FPS",sc.FPS)
    //               // doc.put("TBT",sc.TBT)
    //               // doc.put("APL",sc.APL)
    //               // doc.put("PPS",sc.PPS)
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


    // ConnCountQuery.awaitTermination()
  }
}

