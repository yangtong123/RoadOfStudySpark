package com.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangtong on 17/6/7.
  */
object GroupTop3 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
                .setAppName("GroupTop3")
                .setMaster("local[2]")
        val sc = new SparkContext(conf)
        
        val lines = sc.textFile("./data/core/score.txt")
        val pairs = lines.map{line => {
            val lineSplited = line.split(" ")
            (lineSplited(0), lineSplited(1).toInt)}}
        
        val groupedPair: RDD[(String, Iterable[Int])] = pairs.groupByKey()
        
        val groupSorted: RDD[(String, List[Int])] = groupedPair.map{ pair => {
            val top3 = Array[Int](-1, -1, -1)
            
            val group = pair._1
            val iterator = pair._2.iterator
            
            var flag = true
            while (iterator.hasNext) {
                val score = iterator.next()
                flag = true
                for (i <- 0 until 3 if flag) {
                    if (top3(i) == -1) {
                        top3(i) = score
                        flag = false
                    } else if (score > top3(i) && flag) {
                        for (j <- Range(2, i+1, -1)) {
                            top3(j) = top3(j-1)
                        }
                        top3(i) = score
                        flag = false
                    }
                }
            }
            
            (group, top3.toList)
        }}
    
        groupSorted.foreach{group => {
            print(group._1 + " ")
            group._2.foreach(x => print(x + " "))
            println
        }}
        
    }
}
