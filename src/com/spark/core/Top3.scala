package com.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangtong on 17/6/7.
  */
object Top3 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
                .setAppName("top3")
                .setMaster("local[2]")
        val sc = new SparkContext(conf)
        
        val lines = sc.textFile("./data/core/top.txt")
        val pairs = lines.map(line => (line.toInt, line))
        val sortedPairs = pairs.sortByKey(false)
        val sortedNumbers = sortedPairs.map(_._2)
        val top3Number = sortedNumbers.take(3)
        top3Number.foreach(x => print(x + " "))
    }
}
