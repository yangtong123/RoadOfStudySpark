package com.spark.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangtong on 17/6/16.
  */
object WordCount {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(1))
        
        
        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
        
        val words: DStream[String] = lines.flatMap(_.split(" "))
        
        val pairs: DStream[(String, Int)] = words.map((_, 1))
        
        val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)
        
        wordCounts.print()
        
        
        ssc.start()
        ssc.awaitTermination()
        ssc.stop(false) // false表示不关闭SparkContext
    }
}
