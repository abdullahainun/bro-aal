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
            .add("transactionId", "string")
            .add("customerId", "string")
            .add("itemId", "string")
            .add("amountPaid", "string")

        val fileStreamDf = sparkSession.readStream
            .option("header", "true")
            .option("sep", ",")
            .schema(mySchema)
            .format("csv")
            .load("/home/hduser/aal/bro-aal/src/main/resources/sales.csv")

        fileStreamDf.printSchema

        val query = fileStreamDf
        .writeStream
        .format("console")
        .outputMode("append")
        .start()

        query.awaitTermination()
    }
}
