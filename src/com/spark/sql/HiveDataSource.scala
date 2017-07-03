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
            "LOCAL INPATH './data/sql/student_infos.txt' " +
            "INTO TABLE student_infos")
    
        sparkSession.sql("DROP TABLE IF EXISTS student_scores");
        sparkSession.sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING, score INT)");
        sparkSession.sql("LOAD DATA "
                + "LOCAL INPATH './data/sql/student_scores.txt' "
                + "INTO TABLE student_scores");
    
        val goodStudentsDF = sparkSession.sql("SELECT si.name, si.age, ss.score "
                + "FROM student_infos si "
                + "JOIN student_scores ss ON si.name=ss.name "
                + "WHERE ss.score>=80");
    
        sparkSession.sql("DROP TABLE IF EXISTS good_student_infos");
        goodStudentsDF.write.saveAsTable("good_student_infos")
        
        val goodStudentRows = sparkSession.table("good_student_infos").collect();
        for(goodStudentRow <- goodStudentRows) {
            println(goodStudentRow);
        }
        
    }
}
