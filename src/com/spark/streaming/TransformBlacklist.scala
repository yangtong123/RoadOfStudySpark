package com.spark.streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by yangtong on 17/6/17.
  */
object TransformBlacklist {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(3))
        
        val blacklist = ArrayBuffer[(String, Boolean)](
            ("zhangsan", true),
            ("lisi", true))
        
        val blacklistRDD = ssc.sparkContext.parallelize(blacklist)
        
        val adsClickLogDStream = ssc.socketTextStream("localhost", 9999)
        
        // (username, date username)
        val userAdsClickLogDStream = adsClickLogDStream.map(adsClickLog => (adsClickLog.split(" ")(1), adsClickLog))
        
        
        val validAdsClickLogDStream = userAdsClickLogDStream.transform {
            userAdsClickLogRDD => {
                val joinedRDD = userAdsClickLogRDD.leftOuterJoin(blacklistRDD)
                val filteredRDD = joinedRDD.filter(!_._2._2.getOrElse(false))
                val validAdsClickLogRDD = filteredRDD.map(_._2._1)
                validAdsClickLogRDD
            }
        }
    
        validAdsClickLogDStream.print()
    }
}
