package com.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangtong on 17/6/17.
  */
object KafkaDirectWordCount {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(1))
    
        val kafkaParams = Map[String, String](
            "metadata.broker.list" -> "spark02:9092,spark03:9092"
        )
        
        // 需要读取topic，可以并行读取多个topic
        val topics = Set[String]("WordCount")
        
        val lines: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc,
            kafkaParams,
            topics
        )
        
        val words = lines.flatMap(_._2.split(" "))
        
        val pairs = words.map((_, 1))
        
        val wordCounts = pairs.reduceByKey(_ + _)
        
        wordCounts.print()
        
        ssc.start()
        ssc.awaitTermination()
    }
}
