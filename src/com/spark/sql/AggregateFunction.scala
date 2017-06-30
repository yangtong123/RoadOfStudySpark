package com.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * 聚合函数
  * Created by yangtong on 17/6/30.
  */
object AggregateFunction {
    
    
    def main(args: Array[String]): Unit = {
        // 创建SparkSession
        val spark = SparkSession
                .builder()
                .appName("AggregateFunction")
                .master("local[2]")
                .config("spark.sql.warehouse.dir", "./data/spark-warehouse")
                .getOrCreate()
    
        import org.apache.spark.sql.functions._
        import spark.implicits._
    
        val employee = spark.read.json("./data/sql/employee.json")
        val department = spark.read.json("./data/sql/department.json")
    
        employee
            .join(department, $"depId" === $"id")
            .groupBy(department("name"))
            .agg(avg(employee("salary")), sum(employee("salary")), max(employee("salary")), min(employee("salary")), count(employee("name")), countDistinct(employee("name")))
            .show()
        
        employee
            .groupBy(employee("depId"))
            .agg(collect_list(employee("name")), collect_set(employee("name")))
            .collect()
            .foreach(println)
    }
}
