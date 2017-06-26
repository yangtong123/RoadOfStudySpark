package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 与spark sql整合使用，top3热门商品实时统计
  * Created by yangtong on 17/6/20.
  */
object Top3HotProduct {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("Top3HotProduct")
//        val sc = new SparkContext(conf)
        val sparkSession = SparkSession.builder().config(conf).getOrCreate()
        val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(5))
        
        val productClickLogsDStream = ssc.socketTextStream("localhost", 9999)
        
        // 映射成每个种类的每个商品 (category_product, 1)
        val categoryProductPairsDStream: DStream[(String, Int)] = productClickLogsDStream.map(
            productClickLog => {
                val productClickLogSplited = productClickLog.split(" ")
                (productClickLogSplited(2) + " " + productClickLogSplited(1), 1)
            }
        )
        
        // 执行window操作
        val categoryProductCountsDStream =
            categoryProductPairsDStream.reduceByKeyAndWindow(_+_, _-_, Seconds(60), Seconds(10))
        
        // 然后针对60秒内的每个种类的每个商品点击次数
        // foreachRDD，在内部，使用spark sql执行
        categoryProductCountsDStream.foreachRDD(categoryProductCountsRDD => {
            // 转为RDD[Row]格式
            val categoryProductCountRowRDD: RDD[Row] = categoryProductCountsRDD.map(
                categoryProductCounts => {
                    val category = categoryProductCounts._1.split(" ")(0)
                    val product = categoryProductCounts._1.split(" ")(1)
                    val count = categoryProductCounts._2
                    Row(category, product, count)
                })
            
            // 然后执行DataFrame转换
            val schema = StructType(Array(
                StructField("category", DataTypes.StringType, true),
                StructField("product", DataTypes.StringType, true),
                StructField("click_count", DataTypes.IntegerType, true)
            ))
            
//            val sqlContext = new SQLContext(sparkSession.sparkContext)
            
            val categoryProductCountDF = sparkSession.createDataFrame(categoryProductCountRowRDD, schema)
            
            // 注册为临时表
            categoryProductCountDF.createOrReplaceTempView("product_click_log")
            
            // 执行sql语句
            val top3ProductDF = sparkSession.sql(
                "select category, product, click_count " +
                "from ( " +
                    "select " +
                        "category, " +
                        "product, " +
                        "click_count, " +
                        "row_number() over (partition by category order by click_count desc) rank " +
                    "from product_click_log " +
                ") tmp " +
                "where rank <= 3"
            )
    
            top3ProductDF.show()
        })
        
        
        ssc.checkpoint("./data/checkpoint")
        
        ssc.start()
        ssc.awaitTermination()
    }
}
