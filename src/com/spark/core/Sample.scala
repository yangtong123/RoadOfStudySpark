package com.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangtong on 17/6/26.
  */
object Sample {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
                .setAppName("MapPartitionWithIndex")
                .setMaster("local[1]")
        val sc = new SparkContext(conf)
    
        val staffList = List("张三", "李四", "王五", "赵六", "张三_1", "李四_1", "王五_1", "赵六_1", "王五_2", "赵六_2")
        val staffRDD = sc.parallelize(staffList, 2)
        
        // sample算子，随机抽取
        val luckBoy = staffRDD.sample(false, 0.1)
        
        luckBoy.foreach(println)
        
    }
}
