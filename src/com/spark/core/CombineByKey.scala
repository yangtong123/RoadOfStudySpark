package com.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangtong on 17/7/10.
  */
object CombineByKey {
    
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
                .setAppName("AggregateByKey")
                .setMaster("local[1]")
        val sc = new SparkContext(conf)
        
    
        val data = Array((1, 1.0), (1, 2.0), (1, 3.0), (2, 4.0), (2, 5.0), (2, 6.0))
        val rdd = sc.parallelize(data, 2)
        val combine1 = rdd.combineByKey(
            createCombiner = (v:Double) => (v:Double, 1),
            mergeValue = (c:(Double, Int), v:Double) => (c._1 + v, c._2 + 1),
            mergeCombiners = (c1:(Double, Int), c2:(Double, Int)) => (c1._1 + c2._1, c1._2 + c2._2),
            numPartitions = 2 )
        combine1.foreach(println)
    }
}
