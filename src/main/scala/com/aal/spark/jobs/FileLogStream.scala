package com.aal.spark.jobs

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import com.aal.spark.utils._
object FileLogStream extends StreamUtils {
    def main(args: Array[String]): Unit = {
        
        val sparkSession = getSparkSession(args)
        import sparkSession.implicits._

        // val schema = StructType(
        //     Array(StructField("transactionId", StringType),
        //     StructField("customerId", StringType),
        //     StructField("itemId", StringType),
        //     StructField("amountPaid", StringType))
        // )

        val mySchema = new StructType()
            .add("ts", "string")
            .add("uid", "string")
            .add("id.orig_h", "string")
            .add("id.orig_p", "string")
            .add("id.resp_h", "string")
            .add("id.resp_p", "string")
            .add("proto", "string")
            .add("service", "string")
            .add("duration", "string")
            .add("orig_bytes", "string")
            .add("resp_bytes", "string")
            .add("conn_state", "string")
            .add("local_orig", "string")
            .add("local_resp", "string")
            .add("missed_bytes", "string")
            .add("history", "string")
            .add("orig_pkts", "string")
            .add("orig_ip_bytes", "string")
            .add("resp_pkts", "string")
            .add("resp_ip_bytes", "string")
            .add("tunnel_parents", "string")
            
        val fileStreamDf = sparkSession.readStream
            .option("header", "true")
            .option("sep", " ")
            .schema(mySchema)
            .format("text")
            .load("hdfs://10.252.108.22:9000/user/hduser/ainun/")

        fileStreamDf.printSchema

        val query = fileStreamDf
        .writeStream
        .format("console")
        .outputMode("append")
        .start()

        query.awaitTermination()
    }
}
