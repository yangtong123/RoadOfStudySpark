package com.spark.streaming

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 基于kafka的实时wordcount
  * Created by yangtong on 17/6/16.
  */
object KafkaReceiverWordCount {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(1))
    
        
        val zkQuorum = "192.168.56.101:2181,192.168.56.102:2181,192.168.56.103:2181"
        val consumerGroup = "DefaultConsumerGroup"
        val topicThreadMap = Map("WordCount" -> 1)
        
        //
        val lines = KafkaUtils.createStream(ssc, zkQuorum, consumerGroup, topicThreadMap)
        
        val words = lines.flatMap(_._2.split(" "))
        
        val pairs = words.map((_, 1))
        
        val wordCounts = pairs.reduceByKey(_ + _)
        
    
        ssc.start()
        ssc.awaitTermination()
        ssc.stop(false) // false表示不关闭SparkContext
    }
}
