package com.spark.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * Created by yangtong on 17/6/15.
  */
object RDD2DataFrameProgramatically {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession.builder()
                .appName("RDD2DataFrameProgramatically")
                .master("local[2]")
                .getOrCreate()
        
        
        // 第一步：构造出元素RDD
        val studentRDD = sparkSession.sparkContext.textFile("./data/sql/students.txt")
                .map(line => line.split(","))
                .map(row => Row(row(0).toInt, row(1), row(2).toInt))
        
        // 第二步：编程方式动态构造元数据
        val structType = StructType(Array(
            StructField("id", IntegerType, true),
            StructField("name", StringType, true),
            StructField("age", IntegerType, true)))
        
        // 第三步：进行RDD到DataFrame的转换
        val studentDF = sparkSession.createDataFrame(studentRDD, structType)
        
        
        studentDF.write.mode(SaveMode.Append).save("./data/sql/save/students")
        
        studentDF.show()
        
    }
}
