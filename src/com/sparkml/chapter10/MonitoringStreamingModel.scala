package com.sparkml.chapter10

import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by yangtong on 17/4/19.
  */
object MonitoringStreamingModel {
  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext("local[2]", "First Streaming App", Seconds(10))
    val stream = ssc.socketTextStream("localhost", 9999)

    val NumFeatures = 100
    val zeroVector = DenseVector.zeros[Double](NumFeatures)
    val model1 = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.dense(zeroVector.data))
      .setNumIterations(1)
      .setStepSize(0.01)

    val model2 = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.dense(zeroVector.data))
      .setNumIterations(1)
      .setStepSize(1.0)

    //
    val labeledStream = stream.map { event =>
      val split = event.split(",")
      val y = split(0).toDouble
      val features = split(1).split(",").map(_.toDouble)
      LabeledPoint(label = y, features = Vectors.dense(features))
    }

    model1.trainOn(labeledStream)
    model2.trainOn(labeledStream)

    val predsAndTrue = labeledStream.transform { rdd =>
      val latest1 = model1.latestModel()
      val latest2 = model2.latestModel()
      rdd.map { point =>
        val pred1 = latest1.predict(point.features)
        val pred2 = latest2.predict(point.features)
        (pred1 - point.label, pred2 - point.label)
      }

    }

    predsAndTrue.foreachRDD { (rdd, time) =>
      val mse1 = rdd.map { case (err1, err2) => err1 * err1}.mean()
      val rmse1 = math.sqrt(mse1)

      val mse2 = rdd.map { case (err1, err2) => err2 * err2}.mean()
      val rmse2 = math.sqrt(mse2)

      println(
        s"""
           |-------------------------------------------
           |Time: $time
           |-------------------------------------------
         """.stripMargin
      )

      println(s"MSE current batch: Model 1: $mse1; Mode 2: $mse2")
      println(s"RMSE current batch: Model 1: $rmse1; Mode 2: $rmse2")
      println("...\n")
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
