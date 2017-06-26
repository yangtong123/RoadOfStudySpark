package com.sparkml.chapter10

import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by yangtong on 17/4/19.
  */
object SimpleStreamingModel {
  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext("local[2]", "First Streaming App", Seconds(10))
    val stream = ssc.socketTextStream("localhost", 9999)

    val NumFeatures = 100
    val zeroVector = DenseVector.zeros[Double](NumFeatures)
    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.dense(zeroVector.data))
      .setNumIterations(1)
      .setStepSize(0.1)

    //创建一个标签点的流
    val labeledStream = stream.map { event =>
      val split = event.split("\t")
      val y = split(0).toDouble
      val features = Vectors.dense(split(1).split(",").map(_.toDouble))
      LabeledPoint(label = y, features = features)
    }



    model.trainOn(labeledStream)

    model.predictOn(labeledStream.transform{ rdd => rdd.map {point => point.features}}).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
