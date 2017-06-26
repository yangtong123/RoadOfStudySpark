package com.sparkml.chapter04

//import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.mllib.evaluation.{RankingMetrics, RegressionMetrics}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix

/**
  * Created by yangtong on 17/4/10.
  */
object Chapter04 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark ML Chapter04").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rawData = sc.textFile("./data/ml-100k/u.data")
    rawData.first()

    //得到Rating
    //使用split切割之后会返回一个Array[String]数组。之后调用
    //scala的take函数来仅保留数组的前三个元素
    val rawRatings = rawData.map(_.split("\t").take(3))
    val ratings = rawRatings.map{ case Array(user, movie, rating) =>
      Rating(user.toInt, movie.toInt, rating.toDouble)
    }

    /**
      * rank 对应ALS模型中的因子个数，也就是在低阶近似矩阵中的隐含特征个数。通常合理取值是10到200
      * iterations 迭代次数，一般10次左右
      * lambda 该参数控制模型的正则化过程，从而控制模型过拟合情况。其值越高，正则化越严厉。通过测试和交叉验证法来设置
      *
      * 返回一个MatrixFactorizationModel对象，该对象将用户因子和物品因子分别保存在一个(id, factor)对类型的RDD中。
      * 它们分别称作userFeatures和productFeatures
      */
    val model = ALS.train(ratings, 50, 10, 0.01)


    /**
      * 用户推荐
      */
    //    val predictedRating = model.predict(789, 123)
    val userId = 789
    val K = 10
    val topKRecs = model.recommendProducts(userId, K)
//    println(topKRecs.mkString("\n"))

    //检验推荐内容
    val movies = sc.textFile("./data/ml-100k/u.item")
    val titles = movies.map(line => line.split("\\|").take(2)).map(array =>
      (array(0).toInt, array(1))
    ).collectAsMap()
    titles(123)

    val moviesForUser = ratings.keyBy(_.user).lookup(789)  //lookup取特定用户
    println(moviesForUser.size)

    moviesForUser.sortBy(-_.rating).take(10).map(rating => (titles(rating.product), rating.rating)).foreach(println)


    /**
      * 物品推荐
      */
    //val aMatrix = new DoubleMatrix(Array(1.0, 2.0, 3.0)) 展示用法
    //consineSimilarity函数
    def consineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
      vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
    }
    val itemId = 567
    val itemFactor = model.productFeatures.lookup(itemId).head //lookup返回一个数组而我们只需第一个值
    val itemVector = new DoubleMatrix(itemFactor)
//    consineSimilarity(itemVector, itemVector) //展示用法
    //现在求各个物品与567的余弦相似度
    val sims = model.productFeatures.map {
      case (id, factor) =>
        val factorVector = new DoubleMatrix(factor)
        val sim = consineSimilarity(factorVector, itemVector)
        (id, sim)
    }


//    val sortedSims = sims.top(K)(Ordering.by[(Int, Double), Double] {
//      case (id, similarity) => similarity
//    })
//    println(sortedSims.take(10).mkString("\n"))

    val sortedSims2 = sims.top(K+1)(Ordering.by {
      case (id, similarity) => similarity
    })
    sortedSims2.slice(1, 11).map{
      case (id, sim) => (titles(id), sim)
    }.mkString("\n").foreach(print)


    /**
      * 推荐模型评估效果
      */
    //均方差MSE
    val usersProducts = ratings.map {
      case Rating(user, product, rating) =>
        (user, product)
    }
    val predicttions = model.predict(usersProducts).map {
      case Rating(user, product, rating) =>
        ((user, product), rating)
    }

    val ratingsAndPredictions = ratings.map {
      case Rating(user, product, rating) =>
        ((user, product), rating)
    }.join(predicttions)

    val MSE = ratingsAndPredictions.map {
      case ((user, product), (actual, predicted)) =>
        math.pow((actual-predicted), 2)
    }.reduce(_ + _) / ratingsAndPredictions.count

    println("Mean Squared Error = " + MSE)

    //均方根误差RMSE
    val RMSE = math.sqrt(MSE)
    println("Root Mean Squared Error = " + RMSE)


    //K值平均准确率MAPK是整个数据集上的APK均值
    //计算APK
    def avgPrecisionK(actual: Seq[Int], predicted: Seq[Int], k: Int): Double = {
      val predK = predicted.take(k)
      var score = 0.0
      var numHits = 0.0
      for ((p,i) <- predK.zipWithIndex) {
        if (actual.contains(p)) {
          numHits += 1.0
          score += numHits / (i.toDouble + 1.0)
        }
      }
      if (actual.isEmpty) {
        1.0
      } else {
        score / scala.math.min(actual.size, k).toDouble
      }
    }

    val actualMovies = moviesForUser.map(_.product)
    val predictedMovies = topKRecs.map(_.product)
    val apk10 = avgPrecisionK(actualMovies, predictedMovies, 10)
    println(apk10)

    //计算全局的MAPK，需要计算对每个用户的APK得分，再求其平均。这就要为每一个用户都生成相应的推荐列表。
    //每个工作节点都需要完整的物品因子矩阵。这样它们才能独立地计算某个物品向量与其他所有物品向量之间的相关性
    val itemFactors = model.productFeatures.map {
      case (id, factor) => factor
    }.collect()
    val itemMatrix = new DoubleMatrix(itemFactors)

    val imBroadcast = sc.broadcast(itemMatrix)

    //现在计算每个用户的推荐
    val allRecs: RDD[(Int, Seq[Int])] = model.userFeatures.map {
      case (userId, array) =>
        val userVector = new DoubleMatrix(array)
        val scores = imBroadcast.value.mmul(userVector)  //用户因子矩阵和电影因子矩阵相乘，其结果为一个表示各个电影预计评级的向量
        val sortedWithId = scores.data.zipWithIndex.sortBy(-_._1) //根据预计评级进行排序
        val recommendedIds = sortedWithId.map(_._2 + 1).toSeq //物品因子矩阵的编号从0开始，而我们的电影编号从1开始
        (userId, recommendedIds)
    }
//    allRecs.foreach(println)

    val userMovies: RDD[(Int, Iterable[(Int, Int)])] = ratings.map {
      case Rating(user, product, rating) =>
        (user, product)
    }.groupBy(_._1)

//    val K = 10
    val MAPK = allRecs.join(userMovies).map {
      case (userId, (predicted, actualWithIds)) =>
        val actual = actualWithIds.map(_._2).toSeq
        avgPrecisionK(actual, predicted, K)
    }.reduce(_ + _) / allRecs.count

    println("Mean Average Precision at K = " + MAPK)


    /**
      * 使用MLib内置的评估函数
      */
    //1.RMSE和MSE
    val predictedAndTrue = ratingsAndPredictions.map {
      case ((user, product), (predicted, actual)) =>
        (predicted, actual)
    }
    val regressionMetrics = new RegressionMetrics(predictedAndTrue)
    println("Mean Squared Error = " + regressionMetrics.meanSquaredError)
    println("Root Mean Squared Error + " + regressionMetrics.rootMeanSquaredError)

    //MAP
    val predictedAndTrueForRanking = allRecs.join(userMovies).map {
      case (userId, (predicted, actualWithIds)) =>
        val actual = actualWithIds.map(_._2)
        (predicted.toArray, actual.toArray)
    }
    val rankingMetrics = new RankingMetrics(predictedAndTrueForRanking)
    println("Mean Average precision = " + rankingMetrics.meanAveragePrecision)

    val MAPK2000 = allRecs.join(userMovies).map {
      case (userId, (predicted, actualWithIds)) =>
        val actual = actualWithIds.map(_._2).toSeq
        avgPrecisionK(actual, predicted, 2000)
    }.reduce(_ + _) / allRecs.count

    println("Mean Average Precision = " + MAPK2000)
  }


}




