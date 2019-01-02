package com.aal.spark.jobs
import com.aal.spark.utils._

// svm package
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils

object SimpleSVM extends StreamUtils {
    def main(args: Array[String]): Unit = {
        // Load training data in LIBSVM format.
        val data = MLUtils.loadLibSVMFile(sc, "/home/aal/bro-aal/sample_libsvm_data.txt")

        println("hello")
    }
}