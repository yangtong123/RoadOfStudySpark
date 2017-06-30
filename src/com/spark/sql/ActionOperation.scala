package com.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by yangtong on 17/6/30.
  */
object ActionOperation {
    def main(args: Array[String]): Unit = {
        // 创建SparkSession
        val spark = SparkSession
                .builder()
                .appName("ActionOperation")
                .master("local[2]")
                .config("spark.sql.warehouse.dir", "./data/spark-warehouse")
                .getOrCreate()
    
        import spark.implicits._
        
        val employee = spark.read.json("./data/sql/employee.json")
    
        // collect：将分布式存储在集群上的分布式数据集（比如dataset），中的所有数据都获取到driver端来
        employee.collect().foreach { println(_) }
        // count：对dataset中的记录数进行统计个数的操作
        println(employee.count())
        // first：获取数据集中的第一条数据
        println(employee.first())
        // foreach：遍历数据集中的每一条数据，对数据进行操作，这个跟collect不同，collect是将数据获取到driver端进行操作
        // foreach是将计算操作推到集群上去分布式执行
        // foreach(println(_))这种，真正在集群中执行的时候，是没用的，因为输出的结果是在分布式的集群中的，我们是看不到的
        employee.foreach { println(_) }
        // reduce：对数据集中的所有数据进行归约的操作，多条变成一条
        // 用reduce来实现数据集的个数的统计
        println(employee.map(employee => 1).reduce(_ + _)) // 隐式转换
        // show，默认将dataset数据打印前20条
        employee.show()
        // take，从数据集中获取指定条数
        employee.take(3).foreach { println(_) }
    }
}
