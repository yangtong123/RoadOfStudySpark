package com.spark.streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}

/**
  * 热点搜索词滑动统计，每隔10秒，统计最近60秒钟的搜索词的搜索频次
  * Created by yangtong on 17/6/19.
  */
object WindowHotWord {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(1))
        
        // 日志的格式
        // zhangsan hello
        // lisi world
        val searchLogsDStream = ssc.socketTextStream("localhost", 9999)
        
        // 将搜索日志转换为只有一个搜索词，即可
        val searchWordDStream = searchLogsDStream.map(searchLog => searchLog.split(" ")(1))

        val searchWordPairDStream = searchWordDStream.map((_, 1))
        
        // (searchWord, 1)
        val searchWordCountsDStream = searchWordPairDStream.reduceByKeyAndWindow(_+_, _-_, Durations.seconds(60), Durations.seconds(10))
        
        // 执行transform操作,一个窗口就是一个60秒的数据，会变成一个RDD，然后，对这个RDD根据每个搜索词出现的频率进行排序
        val finalDStream = searchWordCountsDStream.transform {searchWordCountsRDD => {
            val countSearchWordRDD = searchWordCountsRDD.map(x => (x._2, x._1))
            val sortedCountSearchWordsRDD = countSearchWordRDD.sortByKey(false)
            val sortedSearchWordCountsRDD = sortedCountSearchWordsRDD.map(tuple => (tuple._2, tuple._1))
            val top3SearchWordCounts = sortedSearchWordCountsRDD.take(3)
            for (tuple <- top3SearchWordCounts) {
                println(tuple)
            }
            searchWordCountsRDD
        }}
    
        finalDStream.print()
        
        ssc.start()
        ssc.awaitTermination()
    }
}
