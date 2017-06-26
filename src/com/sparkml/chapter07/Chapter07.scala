package com.sparkml.chapter07

import breeze.linalg.{DenseVector, sum}
import breeze.numerics.pow
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangtong on 17/4/17.
  */
object Chapter07 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Chapter07").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val movies = sc.textFile("./data/ml-100k/u.item")

    /**
      * 1.提取特征
      */

    //1.提取电影的题材标签
    val genres = sc.textFile("./data/ml-100k/u.genre")
    val genreMap = genres.filter(!_.isEmpty).map(line => line.split("\\|")).map(array => (array(1), array(0))).collectAsMap()
    //对每部电影提取相应的题材
    val titlesAndGenres = movies.map(_.split("\\|")).map { array =>
      val genres = array.toSeq.slice(5, array.size) //截取从5开始一直到数组结尾的数据
      val genresAssigned = genres.zipWithIndex.filter { case (g, idx) =>
        g == "1" //如果是g是1的话就提取出来
      }.map { case (g, idx) =>
        genreMap(idx.toString) //通过索引得到相应的分类
      }
      (array(0).toInt, (array(1), genresAssigned))
    }

    //2.训练推荐模型
    val rawData = sc.textFile("./data/ml-100k/u.data")
    val rawRating = rawData.map(_.split("\t").take(3))
    val ratings = rawRating.map { case Array(user, movie, rating) =>
      Rating(user.toInt, movie.toInt, rating.toDouble)
    }
    ratings.cache()
    val alsModel = ALS.train(ratings, 50, 10, 0.1) //返回两个RDD，userFeatures和productFeatures
    //提取相关因素
    val movieFactors = alsModel.productFeatures.map { case (id, factor) =>
      (id, Vectors.dense(factor))
    }
    val movieVectors = movieFactors.map(_._2)
    val userFactors = alsModel.userFeatures.map { case (id, factor) =>
      (id, Vectors.dense(factor))
    }
    val userVectors = userFactors.map(_._2)

    //3.归一化
    //在训练聚类模型之前，有必要观察一下输入数据的相关因素特征向量的分布
    val movieMatrix = new RowMatrix(movieVectors)
    val movieMatrixSummary = movieMatrix.computeColumnSummaryStatistics()
    val userMatrix = new RowMatrix(userVectors)
    val userMatrixSummary = userMatrix.computeColumnSummaryStatistics()
    println("Movie factors mean: " + movieMatrixSummary.mean)
    println("Movie factors variance: " + movieMatrixSummary.variance)
    println("User factors mean: " + userMatrixSummary.mean)
    println("User factors variance: " + userMatrixSummary.variance)

    /**
      * 2.训练聚类模型
      */
    val numClusters = 5
    val numIterations = 10
    val numRuns = 3

    val movieClusterModel = KMeans.train(movieVectors, numClusters, numIterations, numRuns)
    //可以再把迭代次数设置多一些
    //得到结论：当我们使用较小的迭代次数进行多次训练时，通常得到的训练误差和已经收敛的模型结果类似。

    val userClusterModel = KMeans.train(userVectors, numClusters, numIterations, numRuns)


    /**
      * 3.使用聚类模型进行预测
      */
    //对一个单独的样本进行预测
    val movie1 = movieVectors.first()
    val movieCluster = movieClusterModel.predict(movie1)
    println(movieCluster)

    //通过传入一个RDD[Vector]数组对多个输入样本进行预测
    val predictions = movieClusterModel.predict(movieVectors)
    println(predictions.take(10).mkString(","))

    //解释类别预测
    def computeDistance(v1: DenseVector[Double], v2: DenseVector[Double])
    = sum(pow(v1 - v2, 2))

    val titlesWithFactors = titlesAndGenres.join(movieFactors)
    val moviesAssigned = titlesWithFactors.map { case (id, ((title, genres), vector)) =>
      val pred = movieClusterModel.predict(vector)
      val clusterCentre = movieClusterModel.clusterCenters(pred)
      val dist = computeDistance(DenseVector(clusterCentre.toArray), DenseVector(vector.toArray))
      (id, title, genres.mkString(" "), pred, dist)
    }
    val clusterAssignments = moviesAssigned.groupBy { case (id, title, genres, cluster, dist) =>
      cluster
    }.collectAsMap()

    //枚举每个类簇并输出距离中心距离最近的前20部电影
    for ( (k, v) <- clusterAssignments.toSeq.sortBy(_._1)) {
      println(s"Cluster $k:")
      val m = v.toSeq.sortBy(_._5)
      println(m.take(20).map { case (_, title, genres, _, d) =>
        (title, genres, d)
      }.mkString("\n"))
      println("=====\n")
    }




    /**
      * 4.评估聚类模型的性能
      */
    //1.内部评价指标
    //WCSS、Davies-Bouldi、Dun、轮廓系数
    //2.外部评价指标
    //Rand measure、F-measur、雅卡尔系数

    val movieCost = movieClusterModel.computeCost(movieVectors)
    val userCost = userClusterModel.computeCost(userVectors)
    println("WCSS for movies: " + movieCost)
    println("WCSS for user: " + userCost)


    /**
      * 5.聚类模型参数调优
      */

    //通过交叉验证选择K
    val trainTestSplitMovies = movieVectors.randomSplit(Array(0.6, 0.4), 123)
    val trainMovies = trainTestSplitMovies(0)
    val testMovies = trainTestSplitMovies(1)
    val costsMovies = Seq(2,3,4,5,10,20).map { k =>
      (k, KMeans.train(trainMovies, k, numIterations, numRuns).computeCost(testMovies))
    }
    println("Movie clustering cross-validation:")
    costsMovies.foreach{ case (k, cost) => println(f"WCSS for K=$k id $cost%2.2f")}

    //用户聚类
    val trainTestSplitUsers = userVectors.randomSplit(Array(0.6, 0.4), 123)
    val trainUsers = trainTestSplitUsers(0)
    val testUsers = trainTestSplitUsers(1)
    val costsUsers = Seq(2,3,4,5,10,20).map { k=>
      (k, KMeans.train(trainUsers, k, numIterations, numRuns).computeCost(testUsers))
    }
    println("User clustering cross-validation:")
    costsUsers.foreach{ case (k, cost) => println(f"WCSS for K=$k id $cost%2.2f")}

  }
}
