package com.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * AggregateByKey算子操作
  * Created by yangtong on 17/6/27.
  */
object AggregateByKey {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
                .setAppName("AggregateByKey")
                .setMaster("local[2]")
        val sc = new SparkContext(conf)
    
        val lines = sc.textFile("./data/core/score.txt")
    
        val pairs: RDD[(String, Int)] = lines.flatMap(_.split(" ")).map(x => (x, 1))
    
//        val wordCount = pairs.reduceByKey(_+_)
        // aggregateByKey，分为三个参数
        // reduceByKey认为是aggregateByKey的简化版
        // aggregateByKey最重要的一点是，多提供了一个函数，Seq Function
        // 就是说自己可以控制如何对每个partition中的数据进行先聚合，类似于mapreduce中的，map-side combine
        // 然后才是对所有partition中的数据进行全局聚合
    
        // 第一个参数是，每个key的初始值
        // 第二个是个函数，Seq Function，如何进行shuffle map-side的本地聚合
        // 第三个是个函数，Combiner Function，如何进行shuffle reduce-side的全局聚合
        
        val wordCounts = pairs.aggregateByKey(0)((v1: Int, v2: Int) => (v1 + v2), (v1: Int, v2: Int) => (v1 + v2))
    
        wordCounts.collect().foreach(println)
    }
}
