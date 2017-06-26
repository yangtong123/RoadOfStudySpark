package com.sparkml.chapter05

import org.apache.spark.mllib.classification.{ClassificationModel, LogisticRegressionWithLBFGS, NaiveBayes, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.optimization.{SimpleUpdater, SquaredL2Updater, Updater}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.{Entropy, Gini, Impurity}
//import org.apache.spark.ml.feature.LabeledPoint
//import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangtong on 17/4/11.
  */
object Chapter05 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark ML Chapter05").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rawData = sc.textFile("./data/train_noheader.tsv")
    val records = rawData.map(line => line.split("\t"))

    /**
      * 抽取特征
      */
    val data: RDD[LabeledPoint] = records.map { r =>
      val trimmed = r.map(_.replaceAll("\"", ""))
      val label = trimmed(r.size - 1).toInt
      val features = trimmed.slice(4, r.size - 1).map (d =>
        if (d == "?") 0.0 else d.toDouble
      )
      LabeledPoint(label, Vectors.dense(features))
    }

    data.cache()
    val numData = data.count()

    //因为数据集中包含负的特征值，而朴素贝叶斯模型要求特征值非负，因此需要将负特征值设为0
    val nbData: RDD[LabeledPoint] = records.map { r =>
      val trimmed = r.map(_.replaceAll("\"", ""))
      val label = trimmed(r.size - 1).toInt
      val features = trimmed.slice(4, r.size - 1).map (d =>
        if (d == "?") 0.0 else d.toDouble
      ).map(d => if (d < 0) 0.0 else d)
      LabeledPoint(label, Vectors.dense(features))
    }


    /**
      * 训练分类模型
      */
    val numIterations: Int = 10
    val maxTreeDepth = 5
//    val lrModel1 = LogisticRegressionWithSGD.train(data, numIterations)
    val lor = new LogisticRegressionWithLBFGS()
    lor.optimizer.setNumIterations(numIterations)
    val lrModel = lor.run(data)

    val svmModel = SVMWithSGD.train(data, numIterations)

    val nbModel = NaiveBayes.train(nbData)

    val dtModel = DecisionTree.train(data, Algo.Classification, Entropy, maxTreeDepth)

    /**
      * 使用分类模型
      */
    val dataPoint = data.first()
    val prediction = lrModel.predict(dataPoint.features)
    val trueLabel = dataPoint.label

    val predictions = lrModel.predict(data.map(lp => lp.features))
    predictions.take(5)

    /**
      * 评估分类模型和性能
      * 正确率：训练样本中被正确分类的数目除以总样本数
      * 准确率：真阳性的数目除以真阳性和假阳性的总数。真阳性就是被正确预测的类别为1的样本，假阳性是错误预测为类别为1的样本
      * 召回率：真阳性的数目除以真阳性和假阴性的和。假阴性是类别为1却被预测为0的样本
      * 准确率-召回率曲线(PR)
      *
      * ROC曲线
      * 真阳性率(TPR)：是真阳性的样本数除以真阳性和假阴性的样本数之和。TPR是真阳性样本数占所有正样本的比例。也称为敏感度
      * 假阳性率(FPR)：是假阳性的样本数除以假阳性和真阴性的样本数之和。FPR是假阳性样本数占所有负样本的比例。
      * ROC曲线下的面积表示平均值(AUC)
      */
    val lrTotalCorrect = data.map { point =>
      if (lrModel.predict(point.features) == point.label) 1 else 0
    }.sum()
    val lrAccuracy = lrTotalCorrect / data.count()

    val svmTotalCorrect = data.map { point =>
      if (svmModel.predict(point.features) == point.label) 1 else 0
    }.sum()
    val svmAccuracy = svmTotalCorrect / data.count()

    val nbTotalCorrect = data.map { point =>
      if (nbModel.predict(point.features) == point.label) 1 else 0
    }.sum()
    val nbAccuracy = nbTotalCorrect / data.count()

    //决策树的预测阈值需要明确给出
    val dtTotalCorrect = data.map { point =>
      val score = dtModel.predict(point.features)
      val predicted = if (score > 0.5) 1 else 0
      if (predicted == point.label) 1 else 0
    }.sum()
    val dtAccuracy = dtTotalCorrect / data.count()

    //计算
    val metrics: Seq[(String, Double, Double)] = Seq(lrModel, svmModel).map { model =>
      val scoreAndLabels = data.map { point =>
        (model.predict(point.features), point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (model.getClass.getSimpleName, metrics.areaUnderPR(), metrics.areaUnderROC())
    }

    val nbMetrics: Seq[(String, Double, Double)] = Seq(nbModel).map { model =>
      val scoreAndLabels = nbData.map { point =>
        val score = model.predict(point.features)
        (if (score > 0.5) 1.0 else 0.0, point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (model.getClass.getSimpleName, metrics.areaUnderPR(), metrics.areaUnderROC())
    }

    val dtMetrics: Seq[(String, Double, Double)] = Seq(dtModel).map { model =>
      val scoreAndLabels = data.map { point =>
        val score = model.predict(point.features)
        (if (score > 0.5) 1.0 else 0.0, point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (model.getClass.getSimpleName, metrics.areaUnderPR(), metrics.areaUnderROC())

    }


    val allMetrics = metrics ++ nbMetrics ++ dtMetrics
    allMetrics.foreach { case (m, pr, roc) =>
      println(f"$m, Area under PR: ${pr * 100.0}%%2.4%%, Area under ROC: ${roc * 100.0}%%2.4%%")
    }

    /**
      * 改进模型以及参数优化
      */
    /*
        特征标准化
     */
    val vectors = data.map(lp => lp.features)
    val matrix = new RowMatrix(vectors)
    val matrixSummary = matrix.computeColumnSummaryStatistics() //计算特征矩阵每列的不同统计数据，包括均值和方差

    //使用StandardScaler进行标准化
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(vectors)
    val scaledData = data.map { lp =>
      LabeledPoint(lp.label, scaler.transform(lp.features))
    }

    //对逻辑回归标准化后重新训练
    val lrModelScaled = lor.run(scaledData)
    val lrTotalCorrectScaled = scaledData.map {
      point =>
        if (lrModelScaled.predict(point.features) == point.label) 1 else 0
    }.sum()
    val lrAccuracyScaled = lrTotalCorrectScaled / numData
    val lrPredictionsVsTrue = scaledData.map {
      point => (lrModelScaled.predict(point.features), point.label)
    }
    val lrMetricsScaled = new BinaryClassificationMetrics(lrPredictionsVsTrue)
    val lrPr = lrMetricsScaled.areaUnderPR()
    val lrRoc = lrMetricsScaled.areaUnderROC()
    println(f"${lrModelScaled.getClass.getSimpleName}\n" +
      f"Accuarcy: ${lrAccuracyScaled * 100}%2.4f%%\n" +
      f"Area Under PR: ${lrPr * 100.0}%2.4f%%\n" +
      f"Area Under ROC: ${lrRoc * 100.0}%2.4f%%")


    /*
        其他特征
     */
    //对每个类别做一个索引的映射
    val categories = records.map(r => r(3)).distinct().collect().zipWithIndex.toMap
    val numCategories = categories.size
    println(categories)

    //创建一个长度为14的向量表示类别特征，然后根据每个样本所属类别索引，对相应的维度赋值为1，其他为0。
    val dataCategories = records.map { r =>
      val trimmed = r.map(_.replaceAll("\"", ""))
      val label = trimmed(r.size - 1).toInt
      val categoryIdx = categories(r(3))
      val categoryFeatures = Array.ofDim[Double](numCategories)
      categoryFeatures(categoryIdx) = 1.0
      val otherFeatures = trimmed.slice(4, r.size - 1).map(d => if (d == "?") 0.0 else d.toDouble)
      val features = categoryFeatures ++ otherFeatures
      LabeledPoint(label, Vectors.dense(features))
    }
    //还需要对上述的数据进行标准化
    val scalerCats = new StandardScaler(withMean = true, withStd = true).fit(dataCategories.map(lp => lp.features))
    val scaledDataCats = dataCategories.map(lp =>
      LabeledPoint(lp.label, scalerCats.transform(lp.features))
    )
    //使用扩展后的特征来训练新的回归模型
    val lrModelScaledCats = lor.run(scaledDataCats)
    val lrTotalCorrectScaledCats = scaledDataCats.map { point =>
      if (lrModelScaledCats.predict(point.features) == point.label) 1 else 0
    }.sum()
    val lrAccuracyScaledCats = lrTotalCorrectScaledCats / numData
    val lrPredictionsVsTrueCats = scaledDataCats.map { point =>
      (lrModelScaledCats.predict(point.features), point.label)
    }
    val lrMetricsScaledCats = new BinaryClassificationMetrics(lrPredictionsVsTrueCats)
    val lrPrCats = lrMetricsScaledCats.areaUnderPR()
    val lrRocCats = lrMetricsScaledCats.areaUnderROC()
    println(f"${lrModelScaled.getClass.getSimpleName}\n" +
      f"Accuarcy: ${lrAccuracyScaledCats * 100}%2.4f%%\n" +
      f"Area Under PR: ${lrPrCats * 100.0}%2.4f%%\n" +
      f"Area Under ROC: ${lrRocCats * 100.0}%2.4f%%")




    /*
      使用正确的数据格式
   */
    //只使用类型特征，1-of-k编码的类型特征更符合朴素贝叶斯模型
    val dataNB = records.map { r =>
      val trimmed = r.map(_.replaceAll("\"", ""))
      val label = trimmed(r.size - 1).toInt
      val categoryIdx = categories(r(3))
      val categoryFeatures = Array.ofDim[Double](numCategories)
      categoryFeatures(categoryIdx) = 1.0
      LabeledPoint(label, Vectors.dense(categoryFeatures))
    }
    //重新训练朴素贝叶斯模型
    val nbModelCats = NaiveBayes.train(dataNB)
    val nbTotalCorrectCats = dataNB.map { point =>
      if (nbModelCats.predict(point.features) == point.label) 1 else 0
    }.sum()
    val nbAccuracyCats = nbTotalCorrectCats / numData
    val nbPredictionsVsTrueCats = dataNB.map { point =>
      (nbModelCats.predict(point.features), point.label)
    }
    val nbMetricsCats = new BinaryClassificationMetrics(nbPredictionsVsTrueCats)
    val nbPreCats = nbMetricsCats.areaUnderPR()
    val nbRocCats = nbMetricsCats.areaUnderROC()
    println(f"${nbModelCats.getClass.getSimpleName}\n" +
      f"Accuarcy: ${nbAccuracyCats * 100}%2.4f%%\n" +
      f"Area Under PR: ${nbPreCats * 100.0}%2.4f%%\n" +
      f"Area Under ROC: ${nbRocCats * 100.0}%2.4f%%")



    /*
        模型参数调优
     */
    //1.线性模型
    def trainWithParams(input: RDD[LabeledPoint], regParam: Double, numIterations: Int, updater: Updater, stepSize: Double) = {
      val lr = new LogisticRegressionWithLBFGS
      lr.optimizer.setNumIterations(numIterations).setUpdater(updater).setRegParam(regParam)
      lr.run(input)
    }

    def createMetrics(label: String, data: RDD[LabeledPoint], model: ClassificationModel) = {
      val scoreAndLabels = data.map { point =>
        (model.predict(point.features), point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (label, metrics.areaUnderROC())
    }
    scaledDataCats.cache()

    //(1)迭代
    val iterResults = Seq(1, 5, 10, 50).map { param =>
      val model = trainWithParams(scaledDataCats, 0.0, param, new SimpleUpdater, 1.0)
      createMetrics(s"$param iterations", scaledDataCats, model)
    }
    iterResults.foreach{case (param, auc) => println(f"$param, AUC = ${auc * 100.0}%2.2f%%")}

    //(2)步长
    //LBFGS没有步长

    //(3)正则化
    //正则化通过限制模型的复杂度避免模型在训练数据中过拟合，但正则化太高可能导致模型欠拟合
    //Mlib中可用的正则化形式有如下几个：
    //SimpleUpdate：相当于没有正则化，是逻辑回归的默认配置
    //SquaredL2Update：这个正则项基于权重向量的L2正则化，是SVM模型的默认值
    //L1Updater：这个正则项基于权重向量的L1正则化，会导致得到一个稀疏的权重向量(不需要的权重的值接近0)
    val regResults = Seq(0.001, 0.01, 0.1, 1.0, 10.0).map { param =>
      val model = trainWithParams(scaledDataCats, param, numIterations, new SquaredL2Updater, 1.0)
      createMetrics(s"$param L2 regularization parameter", scaledDataCats, model)
    }
    regResults.foreach{ case (param, auc) => println(f"$param, AUC = ${auc * 100.0}%%2.2%%")}



    //2.决策树
    //(1)树的深度
    def trainDTWithParams(input: RDD[LabeledPoint], maxDepth: Int, impurity: Impurity) = {
      DecisionTree.train(input, Algo.Classification, impurity, maxDepth)
    }

    val dtResultsEntropy = Seq(1, 2, 3, 4, 5, 10, 20).map { param =>
//      val model = trainDTWithParams(data, param, Entropy) //分别使用Entropy和Gini不纯度来计算
      val model = trainDTWithParams(data, param, Gini)
      val scoreAndLabels = data.map { point =>
        val score = model.predict(point.features)
        (if (score > 0.5) 1.0 else 0.0, point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (s"$param tree depth", metrics.areaUnderROC())
    }

    dtResultsEntropy.foreach { case (param, auc) => println(f"$param, SUC = ${auc * 100}%%2.2%%")}



    //3.朴素贝叶斯
    //lambda参数对朴素贝叶斯模型的影响，该参数可以控制相加式平滑(additive smoothing，解决数据中某个类别和某个特征值的组合没有同时出现
    def trainNBWithParams(input: RDD[LabeledPoint], lambda: Double) = {
      val nb = new NaiveBayes
      nb.setLambda(lambda)
      nb.run(input)
    }
    val nbResults = Seq(0.001, 0.01, 0.1, 1.0, 10.0).map { param =>
      val model = trainNBWithParams(dataNB, param)
      val scoreAndLabels = dataNB.map { point =>
        (model.predict(point.features), point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (s"$param lambda", metrics.areaUnderROC())
    }
    nbResults.foreach { case (param, auc) => println(f"$param, AUC = ${auc * 100.0}%%2.2%%")}


    /**
      * 交叉验证
      *
      * 交叉验证的目的是测试模型在未知数据上的性能，交叉验证让我们使用一部分数据训练模型，将另外一部分用来评估模型性能
      */
    val trainTestSplit = scaledDataCats.randomSplit(Array(0.6, 0.4), 123)
    val train = trainTestSplit(0)
    val test = trainTestSplit(1)

    //
    val regResultsTest = Seq(0.0, 0.001, 0.0025, 0.005, 0.01).map { param =>
      val model = trainWithParams(train, param, numIterations, new SquaredL2Updater, 1.0)
      createMetrics(s"$param L2 regularizationn parameter", test, model)
    }
    regResultsTest.foreach { case (param, auc) =>
      println(f"$param, AUC = ${auc * 100}%%2.6%%")
    }

  }



}
