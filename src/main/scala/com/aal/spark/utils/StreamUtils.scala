package com.aal.spark.utils

/**
  * Created by aal on 17/10/18.
  */

import com.mongodb.spark._
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

private[spark] trait StreamUtils {
  def getSparkContext(args: Array[String]): SparkContext = {
    getSparkSession(args).sparkContext
  }

  def getSparkSession(args: Array[String]): SparkSession = {
    val uri: String = args.headOption.getOrElse("mongodb://admin:jarkoM@10.8.0.2:27017/aal.classification?replicaSet=rs0&authSource=admin")

    val conf = new SparkConf()
      .setMaster ("spark://10.8.0.2:7077")
      .setAppName("AnalisaTrafficDNS")
      .set("spark.app.id", "AnalisaTrafficDNS")
      .set("spark.mongodb.input.uri", uri)
      .set("spark.mongodb.output.uri", uri)

    val session = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    MongoConnector(session.sparkContext).withDatabaseDo(WriteConfig(session), {db => db.drop()})
    session
  }
}
