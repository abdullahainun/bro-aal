package com.aal.spark.jobs
import com.aal.spark.utils._

import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
// $example off$

import org.apache.spark.ml.feature.{OneHotEncoderEstimator, VectorAssembler}
import org.apache.spark.ml.classification.GBTClassifier

import org.apache.spark.ml.feature.VectorSizeHint

import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.col

object StreamAnalysis extends StreamUtils {
    def main(args: Array[String]): Unit = {  
        val spark = getSparkSession(args)

        val data = spark.read.parquet("hdfs://10.252.108.22:9000/user/hduser/ainun/tmp/myDecisionTreeClassificationModel/data")
        data.printSchema()

        val oneHot = new OneHotEncoderEstimator()
        .setInputCols(Array("amountRange"))
        .setOutputCols(Array("amountVect"))

        val vectorAssembler = new VectorAssembler()
        .setInputCols(Array("amountVect", "pcaVector"))
        .setOutputCol("features")

        val estimator = new GBTClassifier()
        .setLabelCol("label")
        .setFeaturesCol("features")

        val vectorSizeHint = new VectorSizeHint()
        .setInputCol("pcaVector")
        .setSize(28)

        val Array(train, test) = data.randomSplit(weights=Array(.8, .2))

        val pipeline = new Pipeline()
        .setStages(Array(oneHot, vectorSizeHint, vectorAssembler, estimator))

        val pipelineModel = pipeline.fit(train)

        spark.stop()
    }
}    