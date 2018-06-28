package com.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangtong on 17/6/7.
  */
object WordCount {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
                .setAppName("wordcount")
                .setMaster("local[2]")
        val sc = new SparkContext(conf)
        
        val lines = sc.textFile("./data/core/score.txt")
        
        
        val pairs = lines.flatMap(_.split(" ")).map((_, 1))
        
        
        val wordCount = pairs.reduceByKey(_+_)
        
        val wordSort = wordCount.map(x=>(x._2, x._1)).sortByKey(false).map(x=>(x._2, x._1))
        
        wordSort.collect()
        
        
        
    }
}
