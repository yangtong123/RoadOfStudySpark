package com.spark.streaming

import java.sql.Statement

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 改写UpdateStateByKeyWordCount，将每次统计出来的全局的单词计数，写入一份
  *
  * Created by yangtong on 17/6/19.
  */
object PersistWordCount {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(3))
    
        // 必须设置checkpoint
        val checkpointDir = "./data/checkpoint/"
        ssc.checkpoint(checkpointDir)
    
        val lines = ssc.socketTextStream("localhost", 9999)
    
        val words = lines.flatMap(_.split(" "))
    
        val pairs = words.map((_, 1))
    
        // 统计全局的单词计数
        // 使用updateStateByKey可以维护一份全局的
    
        def updateState(values: Seq[Int], state: Option[Int]) = {
            var newValues = state.getOrElse(0)
            for (value <- values) {
                newValues += value
            }
            Some(newValues)
        }
    
        val wordCounts = pairs.updateStateByKey(updateState)
    
        
        // 写入mysql
        wordCounts.foreachRDD(rdd => {
            rdd.foreachPartition(wordCounts => {
                val conn = ConnectionPool.getConnection()
    
                var wordCount: (String, Int) = null
                
                while (wordCounts.hasNext) {
                    wordCount = wordCounts.next()
                    
                    val sql = "insert into wordcount(word, count) " +
                        "values('" + wordCount._1 + "'," + wordCount._2 + ")";
                    
                    val stmt: Statement = conn.createStatement()
                    
                    stmt.executeUpdate(sql)
                }
                
            })
        })
    
//        wordCounts.print()
    
        ssc.start()
        ssc.awaitTermination()
    }
}
