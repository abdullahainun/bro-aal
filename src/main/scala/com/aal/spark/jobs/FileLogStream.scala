package com.aal.spark.jobs

import java.util.concurrent.TimeUnit

import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import com.aal.spark.utils._
import org.apache.spark.sql.types._

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

        val mySchema : StructType = StructType( 
            Seq(
                StructField("ts", DoubleType, true),
                StructField("uid", StringType, true),
                StructField("id.orig_h", StringType, true),
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
                StructField("resp_ip_bytes", StringType, true),
                StructField("tunnel_parents", StringType, true)
            
            )
        )

        val fileStreamDf = sparkSession.readStream
            .option("header", "true")
            .option("sep", "\t")
            .option("maxFilesPerTrigger",1)
            .schema(mySchema)
            .format("csv")
            .load("hdfs://10.252.108.22:9000/user/hduser/broconn/")

        val parsedRawDf = fileStreamDf
            .withColumn("ts",to_utc_timestamp(from_unixtime(col("ts")),"GMT").alias("ts").cast(StringType))

        val newDF = parsedRawDf  
            .withColumn("PX", BroConnFeatureExtractionFormula.px(col("orig_pkts").cast("int"), col("resp_pkts").cast("int")))
            .withColumn("NNP", BroConnFeatureExtractionFormula.nnp(col("PX").cast("int")))
            .withColumn("NSP", BroConnFeatureExtractionFormula.nsp(col("PX").cast("int")))
            .withColumn("PSP", BroConnFeatureExtractionFormula.psp(col("NSP").cast("double"), col("PX").cast("double")))
            .withColumn("IOPR", BroConnFeatureExtractionFormula.iopr(col("orig_pkts").cast("int"), col("resp_pkts").cast("int")))
            .withColumn("Reconnect", BroConnFeatureExtractionFormula.reconnect(col("history").cast("string")))
            .withColumn("FPS", BroConnFeatureExtractionFormula.fps(col("orig_ip_bytes").cast("int"), col("resp_pkts").cast("int")))
            .withColumn("TBT", BroConnFeatureExtractionFormula.tbt(col("orig_ip_bytes").cast("int"), col("resp_ip_bytes").cast("int")))
            .withColumn("APL", BroConnFeatureExtractionFormula.apl(col("PX").cast("int"), col("orig_ip_bytes").cast("int"), col("resp_ip_bytes").cast("int")))
            .withColumn("PPS", BroConnFeatureExtractionFormula.pps(col("duration").cast("double"), col("orig_pkts").cast("int"), col("resp_pkts").cast("int")))
            .withColumn("label", BroConnLabeling.labeling(col("id_orig_h").cast("string")))

        newDF.printSchema

        val query = newDF
        .writeStream
        .format("console")
        .outputMode("append")
        .start()

        query.awaitTermination()
    }
}
