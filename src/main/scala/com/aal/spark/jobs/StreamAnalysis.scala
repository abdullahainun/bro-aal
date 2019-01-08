package com.aal.spark.jobs
import com.aal.spark.utils._

import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
// $example off$

object StreamAnalysis extends StreamUtils {
    def main(args: Array[String]): Unit = {  
        val spark = getSparkSession(args)

        val data = spark.read.parquet("hdfs://10.252.108.22:9000/user/hduser/ainun/tmp/myDecisionTreeClassificationModel")
        data.printSchema()

        spark.stop()
    }
}    