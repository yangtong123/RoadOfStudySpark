package com.spark.sql

import org.apache.spark.sql.SparkSession


/**
  * 计算部门的平均薪资和年龄
  *
  * 需求：
  *  1、只统计年龄在20岁以上的员工
  *  2、根据部门名称和员工性别为粒度来进行统计
  *  3、统计出每个部门分性别的平均薪资和年龄
  *
  * Created by yangtong on 17/6/30.
  */
object DepartmentAvgSalaryAndAgeStat {
    
    def main(args: Array[String]): Unit = {
        // 创建SparkSession
        val spark = SparkSession
                .builder()
                .appName("DepartmentAvgSalaryAndAgeStat")
                .master("local[2]")
                .config("spark.sql.warehouse.dir", "./data/spark-warehouse")
                .getOrCreate()
    
        // 导入spark的隐式转换
        import spark.implicits._
        // 导入spark sql的functions
        import org.apache.spark.sql.functions._
        
        // 首先将两份数据文件加载进来，形成两个dataframe（讲的是untyped类型的操作入门）
        val employee = spark.read.json("./data/sql/employee.json")
        val department = spark.read.json("./data/sql/department.json")
    
        employee
            // 先对employee进行过滤，只统计20岁以上的员工
            .filter("age > 20")
            // 需要跟department数据进行join，然后才能根据部门名称和员工性别进行聚合
            // 注意：untyped join，两个表的字段的连接条件，需要使用三个等号
            .join(department, $"depId" === $"id")
            // 根据部门名称和员工性别进行分组
            .groupBy(department("name"), employee("gender"))
            // 最后执行聚合函数
            .agg(avg(employee("salary")), avg(employee("age")))
            // 执行action操作，将结果显示出来
            .show()
        
        
    }
    
    
}
