package com.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by yangtong on 17/6/15.
  */
object JSONDataSource {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession.builder()
                .appName("JSONDataSource")
                .master("local[2]")
                .getOrCreate()
        
        val studentScoresDF = sparkSession.read.json("./data/sql/students.json")
        studentScoresDF.createOrReplaceTempView("studentScoresDF")
        
        val goodStudentDF = sparkSession.sql("select * from studentScoresDF where score >= 80")
        
        goodStudentDF.show()
        
        val studentInfoJSONs = Array("{\"name\": \"zhangsan\", \"age\": 18}",
            "{\"name\": \"lisi\", \"age\": 18}",
            "{\"name\": \"tom\", \"age\": 18}")
        val studentInfoJSONsRDD: RDD[String] = sparkSession.sparkContext.parallelize(studentInfoJSONs)
        val studentInfoDF: DataFrame = sparkSession.read.json(studentInfoJSONsRDD)
        
        
        
        // 查询分数大于80分的学生的基本信息
        studentInfoDF.createOrReplaceTempView("student_infos")
    
        
        
        var sql = "select name, age from student_infos where name in ("
    }
}
