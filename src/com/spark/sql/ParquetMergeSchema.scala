package com.spark.sql

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by yangtong on 17/6/15.
  */
object ParquetMergeSchema {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession.builder()
                .appName("ParquetMergeSchema")
                .master("local[2]")
                .getOrCreate()
        
        import sparkSession.implicits._
        
        //
        val studentsWithNameAge = Array(("zhangsan", 22), ("lisi", 23))
        val studentsWithNameAgeDF = sparkSession.sparkContext.parallelize(studentsWithNameAge).toDF("name", "age")
        studentsWithNameAgeDF.write.format("parquet").mode(SaveMode.Append).save("./data/sql/parquet/students")
        
        val studentsWithNameGrade = Array(("yangtong", 99), ("xiaoyi", 100))
        val studentsWithNameGradeDF = sparkSession.sparkContext.parallelize(studentsWithNameGrade).toDF("name", "grade")
        studentsWithNameGradeDF.write.format("parquet").mode(SaveMode.Append).save("./data/sql/parquet/students")
        
        
        val students = sparkSession.read.option("mergeSchema", true).parquet("./data/sql/parquet/students")
        students.printSchema()
        students.show()
    }
}
