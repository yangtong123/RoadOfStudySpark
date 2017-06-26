package com.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by yangtong on 17/6/16.
  */
object DailyTop3Keyword {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession.builder()
                .appName("UDAF")
                .master("local[2]")
                .getOrCreate()
        
        sparkSession.sparkContext.broadcast()
    }
}
