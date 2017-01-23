package com.demo.chapter06

import org.apache.flink.api.scala._
import org.apache.flink.ml.regression.MultipleLinearRegression
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.math.{ SparseVector, DenseVector }

object MyMRLApp {

   def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // Create multiple linear regression learner
    val mlr = MultipleLinearRegression()
      .setIterations(10)
      .setStepsize(0.5)
      .setConvergenceThreshold(0.001)

    // Obtain training and testing data set
    val trainingDS: DataSet[LabeledVector] = // input data
   // val testingDS: DataSet[Vector] = // output data

    // Fit the linear model to the provided data
    mlr.fit(trainingDS)

    // Calculate the predictions for the test data
//    val predictions = mlr.predict(testingDS)
    predictions.writeAsText("mlr-out")

    env.execute("Flink MLR App")
  }
}