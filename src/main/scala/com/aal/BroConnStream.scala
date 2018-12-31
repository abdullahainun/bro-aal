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
                           respIpBytes: Integer
                         )

  def main(args: Array[String]): Unit = {
    val kafkaUrl = "master.pens.ac.id:9092"
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
        StructField("resp_ip_bytes", IntegerType, true))
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

    val parsedLog = parsedLogData.select("conn.*")
    
    

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

    // Print new data to console
     connDf
     .writeStream
      .format("console")
     .start()

    //Sink to Mongodb
    val ConnCountQuery = connDf
      .writeStream
      .format("console")
      .outputMode("append")

      .foreach(new ForeachWriter[ConnCountObj] {

          val writeConfig: WriteConfig = WriteConfig(Map("uri" -> "mongodb://10.252.108.98/bro.connlog"))
          var mongoConnector: MongoConnector = _
          var ConnCounts: mutable.ArrayBuffer[ConnCountObj] = _

          override def process(value: ConnCountObj): Unit = {
            ConnCounts.append(value)
            println(ConnCounts)
          }

          override def close(errorOrNull: Throwable): Unit = {
            if (ConnCounts.nonEmpty) {
              mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>
                collection.insertMany(ConnCounts.map(sc => {
                  var doc = new Document()
                  var px = sc.origPkts + sc.respPkts;
                  var nnp = 0;
                  var nsp = 0;
                  var psp = 0;
                  var iopr = 0;
                  var reconnect = 0;
                  var fps = 0;
                  var tbt = 0;
                  var apl = 0;
                  var dpl = 0;
                  var pv = 0
                  var bs = 0;
                  var ps = 0;
                  var ait = 0;
                  var pps = 0;   

                  // set nnp
                  if( px == 0 ){
                    nnp = 1;
                  }else{
                    nnp = 0;
                  }
                  // set nsp
                  if(px >= 63 && px <= 400 ){
                    nsp = 1;
                  }else{
                    nsp = 0;
                  }
                  // psp
                  psp = nsp/px;
                  if(sc.respPkts != 0){
                    iopr = sc.origPkts / sc.respPkts;
                  }else{
                    iopr = 0
                  }
                  // set reconnect
                  if (sc.history == "ShADadfFr"){
                    reconnect = 1
                  }else{
                    reconnect = 0
                  }
                  // set fps
                  if(sc.origPkts !=0 ){
                    fps = sc.origIpBytes / sc.origPkts                    
                  }else{
                    fps = 0
                  }
                  // set tbt
                  if(sc.respIpBytes != 0){
                    tbt = sc.origIpBytes + sc.respIpBytes                    
                  }else{
                    tbt = 0
                  }

                  if(px != 0){
                    apl = ( sc.origIpBytes + sc.respIpBytes ) / px
                  }
                  // set ps, bs and pps                  
                  if(sc.duration != 0){
                    pps = sc.origPkts + sc.respPkts / sc.duration
                  }

                  doc.put("ts", sc.timestamp)
                  doc.put("uid", sc.uid)
                  doc.put("id_orig_h", sc.idOrigH)
                  doc.put("id_orig_p", sc.idOrigP)
                  doc.put("id_resp_h", sc.idRespH)
                  doc.put("id_resp_p", sc.idRespP)
                  doc.put("duration", sc.duration)
                  doc.put("history", sc.history)
                  doc.put("orig_pkts", sc.origPkts)
                  doc.put("orig_ip_bytes", sc.origIpBytes)
                  doc.put("resp_bytes", sc.respPkts)
                  doc.put("resp_ip_bytes", sc.respIpBytes)
                  doc.put("PX",px)                  
                  doc.put("NNP",nnp)
                  doc.put("NSP",nsp)
                  doc.put("PSP",psp)
                  doc.put("IOPR",iopr)
                  doc.put("Reconnect",reconnect)
                  doc.put("FPS",fps)
                  doc.put("TBT",tbt)
                  doc.put("APL",apl)
                  doc.put("PPS",pps)
                  doc
                }).asJava)
              })
            }
          }

          override def open(partitionId: Long, version: Long): Boolean = {
            mongoConnector = MongoConnector(writeConfig.asOptions)
            println(mongoConnector)
            ConnCounts = new mutable.ArrayBuffer[ConnCountObj]()
            true
          }

    }).start()


    ConnCountQuery.awaitTermination()
  }
}

