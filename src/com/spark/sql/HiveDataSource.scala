package com.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by yangtong on 17/6/15.
  */
object HiveDataSource {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession.builder()
                .appName("HiveDataSource")
                .master("local[2]")
                .enableHiveSupport()
                .getOrCreate()
        
        sparkSession.sql("DROP TABLE IF EXISTS student_infos")
        
        sparkSession.sql("CREATE TABLE IF NOT EXISTS student_infos (name String, age int)")
        
        sparkSession.sql("LOAD DATA " +
            "LOCAL INPATH './data/sql/student_infos' " +
            "INTO TABLE student_infos")
        
        
        
    }
}
