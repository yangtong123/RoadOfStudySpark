package com.spark.core

import java.util

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangtong on 17/6/26.
  */
object MapPartitionWithIndex {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
                .setAppName("MapPartitionWithIndex")
                .setMaster("local[1]")
        val sc = new SparkContext(conf)
        
        val studentNames = List("张三", "李四", "王五", "赵六")
        val studentNamesRDD = sc.parallelize(studentNames, 2)
        
        // mapPartitionWithIndex可以拿到每个partition的index
        val studentNamesWithClass = studentNamesRDD.mapPartitionsWithIndex(
            (index: Int, iterator: Iterator[String]) => Iterator {
//                val studentWithClassList: ArrayBuffer[String] = new ArrayBuffer[String]
                val studentWithClassList = new util.ArrayList[String]()
                while(iterator.hasNext) {
                    val studentName = iterator.next()
                    val studentWithClass = studentName + "_" + (index + 1)
//                    studentWithClass +=: studentWithClassList
                    studentWithClassList.add(studentWithClass)
                }
                studentWithClassList
            }, true);
        
    
        studentNamesWithClass.foreach(println)
        
        
    }
}
