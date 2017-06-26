package com.spark.ml.chapter05

import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangtong on 17/6/6.
  */
object StudyNaiveBayes {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("naive_bayes").setMaster("local[2]")
        val sc = new SparkContext(conf)
        
        // 读取样本数据1
        val data = sc.textFile("./data/sample_naive_bayes_data.txt")
        val parseData = data.map { line =>
            val parts = line.split(',')
            LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
        }
        
        // 样本数据划分训练样本和测试样本
        val splits = parseData.randomSplit(Array(0.6, 0.4), seed = 11L)
        val training = splits(0)
        val test = splits(1)
        
        // 新建贝叶斯分类模型，并训练
        val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")
        model.labels
        model.pi
        model.theta
        model.modelType
        
        
        // 对测试样本进行测试
        val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
        val print_predict = predictionAndLabel.take(20)
        println("prediction" + "\t" + "label")
        for (i <- 0 to print_predict.length - 1) {
            println(print_predict(i)._1 + "\t" + print_predict(i)._2)
        }
        
        val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
        
        // 保存模型
        val ModelPath = "./model/naive_bayes_model"
        model.save(sc, ModelPath)
        val sameModel = NaiveBayesModel.load(sc, ModelPath)
        sameModel.labels
        sameModel.pi
        sameModel.theta
        sameModel.modelType
    }
}
