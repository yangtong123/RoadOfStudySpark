package com.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by yangtong on 17/6/15.
  */

case class Student(id: Int, name: String, age: Int)

object RDD2DataFrameReflection {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession.builder()
                .appName("RDD2DataFrameReflection")
                .master("local[2]")
                .getOrCreate()
        
        
        test(sparkSession)
       
    }
    
    def test(sparkSession: SparkSession): Unit = {
        import sparkSession.sqlContext.implicits._
    
        val students: RDD[Student] = sparkSession.sparkContext.textFile("./data/sql/students.txt")
                .map(line => line.split(","))
                .map(arr => Student(arr(0).trim.toInt, arr(1), arr(2).trim.toInt))
    
        val studentsDF: DataFrame = students.toDF("id", "name", "age")
    
        //        studentsDF.schema
    
        studentsDF.printSchema()
    
        studentsDF.createOrReplaceTempView("students")
    
        val testDF = sparkSession.sql("select * from students")
        
        testDF.show()
    
    }
}
