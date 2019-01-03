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
            List(
                StructField(("ts", StringTyp, true)
                StructField("uid", StringType, true),
                StructField("id.orig_h", StringType, true),
                StructField("id.orig_p", StringType, true),
                StructField("id.resp_h", StringType, true),
                StructField("id.resp_p", StringType, true),
                StructField("proto", StringType, true),
                StructField("service", StringType, true),
                StructField("duration", StringType, true),
                StructField("orig_bytes", StringType, true),
                StructField("resp_bytes", StringType, true),
                StructField("conn_state", StringType, true),
                StructField("local_orig", StringType, true),
                StructField("local_resp", StringType, true),
                StructField("missed_bytes", StringType, true),
                StructField("history", StringType, true),
                StructField("orig_pkts", StringType, true),
                StructField("orig_ip_bytes", StringType, true),
                StructField("resp_pkts", StringType, true),
                StructField("resp_ip_bytes", StringType, true),
                StructField("tunnel_parents", StringType, true)
            )

        val fileStreamDf = sparkSession.readStream
            .option("header", "true")
            .option("sep", " ")
            .schema(mySchema)
            .format("csv")
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
