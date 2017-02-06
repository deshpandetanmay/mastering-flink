package com.demo.flink.ml

import org.apache.flink.api.scala._
import org.apache.flink.ml._
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.preprocessing.Splitter
import org.apache.flink.ml.regression.MultipleLinearRegression
import org.apache.flink.ml.preprocessing.PolynomialFeatures
import org.apache.flink.ml.preprocessing.StandardScaler
import org.apache.flink.ml.preprocessing.MinMaxScaler

/**
 * This class shows how to solve classification problems using Flink ML
 *
 * Machine Learning Algorithm - Multiple Linear Regression
 * Data Pre-processing - Using Standard Scaler and Polynomial Feature
 */
object MLRJobPipelines {
  def main(args: Array[String]) {
    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    // Use polynomial feature with degree 3
    val polyFeatures = PolynomialFeatures()
      .setDegree(3)

    val scaler = StandardScaler()
      .setMean(10.0)
      .setStd(2.0)

    val minMaxscaler = MinMaxScaler()
      .setMin(1.0)
      .setMax(3.0)

    val trainingDataset = MLUtils.readLibSVM(env, "iris-train.txt")
    val testingDataset = MLUtils.readLibSVM(env, "iris-test.txt").map { lv => lv.vector }
    val mlr = MultipleLinearRegression()
      .setStepsize(1.0)
      .setIterations(5)
      .setConvergenceThreshold(0.001)

    // Learn the mean and standard deviation of the training data
    // scaler.fit(trainingDataset)
    minMaxscaler.fit(trainingDataset)

    // Scale the provided data set to have mean=10.0 and std=2.0
    //val scaledDS = scaler.transform(trainingDataset)
    
    val scaledDS = minMaxscaler.transform(trainingDataset)
    
    scaledDS.print()
    // Create pipeline PolynomialFeatures -> MultipleLinearRegression
    val pipeline = polyFeatures.chainPredictor(mlr)

    // train the model
    pipeline.fit(scaledDS)

    // The fitted model can now be used to make predictions
    val predictions = pipeline.predict(testingDataset)

    predictions.print()

  }
}
