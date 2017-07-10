package com.spark.ml.chapter03

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
import org.apache.spark.sql.SparkSession

/**
  * 线性回归
  * Created by yangtong on 17/6/5.
  */
object LinearRegression {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession.builder()
                .appName("LinearRegressionWithSGD")
                .master("local[2]")
                .getOrCreate()
        
        val data_path1 = "./data/lpsa.data"
        val data = sparkSession.sparkContext.textFile(data_path1)
        val examples = data.map { line =>
            val parts = line.split(",")
            LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble)))
        }.cache()
        val numExamples = examples.count
        
        val numIterators = 100
        val stepSize = 1
        val miniBatchFraction = 1.0
        val model = LinearRegressionWithSGD.train(examples, numIterators, stepSize, miniBatchFraction)
        model.weights
        model.intercept
        
        val prediction = model.predict(examples.map(_.features))
        val predictionAndLabel = prediction.zip(examples.map(_.label))
        val print_predict = predictionAndLabel.take(20)
        println("prediction" + "\t" + "label")
        for (i <- 0 to print_predict.length - 1) {
            println(print_predict(i)._1 + "\t" + print_predict(i)._2)
        }
        
        val loss = predictionAndLabel.map {
            case (p, l) =>
                val err = p - l
                err * err
        }.reduce(_ + _)
        val rmse = math.sqrt(loss / numExamples)
        println(s"Test RMSE = $rmse.")
        
        val ModelPath = "./model/LinearRegressionModel"
        model.save(sparkSession.sparkContext, ModelPath)
        val sameModel = LinearRegressionModel.load(sparkSession.sparkContext, ModelPath)
    }
}
