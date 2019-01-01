package com.aal.spark.utils

/**
  * Created by aal on 1/1/19.
  * rumus - rumus feature extraction pada conn log bro - ids
  */
import org.apache.spark.sql.functions.udf

object BroConnFeatureExtractionFormula{
    val px = udf((origPkts: Integer, respPkts: Integer) => origPkts + respPkts )
}