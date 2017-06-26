package com.spark.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by yangtong on 17/6/15.
  */
object DailyUV {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession.builder()
                .appName("HiveDataSource")
                .master("local[2]")
                .getOrCreate()
        
        // 要使用内置函数，就必须导入隐式转换
        import sparkSession.implicits._
        
        val userAccessLog = Array(
            "2017-06-15,1122",
            "2017-06-15,1122",
            "2017-06-15,1123",
            "2017-06-15,1124",
            "2017-06-15,1122",
            "2017-06-16,1124",
            "2017-06-16,1125",
            "2017-06-16,1123",
            "2017-06-16,1125",
            "2017-06-16,1124"
        )
        
        val userAccessLogRDD = sparkSession.sparkContext.parallelize(userAccessLog)
        
        val userAccessLogRowRDD = userAccessLogRDD.map{_.split(",")}
                .map(log => Row(log(0), log(1).trim.toInt))
        
        val structType = StructType(Array(
            StructField("date", StringType, true),
            StructField("userId", IntegerType, true)))
        
        val userAccessLogRowDF: DataFrame = sparkSession.createDataFrame(userAccessLogRowRDD, structType)
    
        userAccessLogRowDF.groupBy("date")
                .agg('date, countDistinct('userId).as("uv"))
                .map{row => (row.getAs[String]("date"), row.getAs[Long]("uv"))}
                .collect()
                .foreach(println)
    }
}
