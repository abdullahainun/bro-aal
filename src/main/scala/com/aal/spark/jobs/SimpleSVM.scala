package com.aal.spark.jobs
import com.aal.spark.utils._

// svm package
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils

object SimpleSVM extends StreamUtils {
    def main(args: Array[String]): Unit = {
        // Load training data in LIBSVM format.
        val spark = getSparkContext(args)
        val data = MLUtils.loadLibSVMFile(spark, "hdfs://127.0.0.1:9000/user/hduser/aal/dataset_isot.data")

        // Split data into training (60%) and test (40%).
        val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
        val training = splits(0).cache()
        val test = splits(1)

        // Run training algorithm to build the model
        val numIterations = 100
        val model = SVMWithSGD.train(training, numIterations)

        // Clear the default threshold.
        model.clearThreshold()

        // Compute raw scores on the test set.
        val scoreAndLabels = test.map { point =>
        val score = model.predict(point.features)
        (score, point.label)
        }

        // Get evaluation metrics.
        val metrics = new BinaryClassificationMetrics(scoreAndLabels)
        val auROC = metrics.areaUnderROC()

        println(s"Area under ROC = $auROC")

        // Save and load model
        model.save(spark, "hdfs://127.0.0.1:9000/user/hduser/aal/tmp/isot-svm-model")
        val sameModel = SVMModel.load(spark, "hdfs://127.0.0.1:9000/user/hduser/aal/tmp/isot-dt-model")

        spark.stop()
    }
}