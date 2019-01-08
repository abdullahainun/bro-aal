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
        val spark = getSparkSession(args)

        // $example on$
        // Load and parse the data file.
        val data = spark.read.format("libsvm").load("hdfs://10.252.108.22:9000/user/hduser/ainun/dataset_isot.data")

        // load data streaming

        val labelIndexer = new StringIndexer()
        .setInputCol("label")
        .setOutputCol("indexedLabel")
        .fit(data)

        // Automatically identify categorical features, and index them.
        val featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexedFeatures")
        .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
        .fit(data)

        // Split the data into training and test sets (30% held out for testing).
        val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

        // Train a DecisionTree model.
        val dt = new DecisionTreeClassifier()
        .setLabelCol("indexedLabel")
        .setFeaturesCol("indexedFeatures")

        // Convert indexed labels back to original labels.
        val labelConverter = new IndexToString()
        .setInputCol("prediction")
        .setOutputCol("predictedLabel")
        .setLabels(labelIndexer.labels)

        // Chain indexers and tree in a Pipeline.
        val pipeline = new Pipeline()
        .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

        // Train model. This also runs the indexers.
        val model = pipeline.fit(trainingData)

        // Now we can optionally save the fitted pipeline to disk
        model.write.overwrite().save("hdfs://10.252.108.22:9000/user/hduser/ainun/tmp/isot-dt-model")
        // Make predictions.
        val predictions = model.transform(testData)

        // Select example rows to display.
        predictions.select("predictedLabel", "label", "features").show(100)

        // Select (prediction, true label) and compute test error.
        val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(predictions)
        println(s"akurasi = ${(accuracy)}")
        println(s"Test Error = ${(1.0 - accuracy)}")

        val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
        println(s"Learned classification tree model:\n ${treeModel.toDebugString}")
        // $example off$

        spark.stop()
    }
}