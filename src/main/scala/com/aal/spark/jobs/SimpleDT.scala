package com.aal.spark.jobs
import com.aal.spark.utils._

import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
// $example off$

object SimpleDT extends StreamUtils {
    def main(args: Array[String]): Unit = {        
        val sc = getSparkContext(args)

        // $example on$
        // Load and parse the data file.
        val data = MLUtils.loadLibSVMFile(sc, "hdfs://10.252.108.22:9000/user/hduser/ainun/dataset_isot.data")
        val labelIndexer = new StringIndexer()
        .setInputCol("label2")
        .setOutputCol("indexedLabel")
        .fit(data)

        sc.stop()
    }
}