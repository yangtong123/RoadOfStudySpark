package com.spark.sql

import java.sql.DriverManager

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable

/**
  * Created by yangtong on 17/6/15.
  */
object JDBCDataSource {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession.builder()
                .appName("JDBCDataSource")
                .master("local[2]")
                .getOrCreate()
        
        val studentsInfosOptions = mutable.Map("url" -> "jdbc:mysql://localhost:3306/spark_sql",
            "user" -> "root",
            "password" -> "542919899",
            "dbtable" -> "students_infos")
        
        val studentsScoresOptions = mutable.Map("url" -> "jdbc:mysql://localhost:3306/spark_sql",
            "user" -> "root",
            "password" -> "542919899",
            "dbtable" -> "students_scores")
        
        val stuentsInfosDF = sparkSession.read.format("jdbc").options(studentsInfosOptions).load
        stuentsInfosDF.show()
        
        val stuentsScoresDF = sparkSession.read.format("jdbc").options(studentsScoresOptions).load
        stuentsScoresDF.show()
    
        val studentsRDD: RDD[(String, (Int, Int))] = stuentsInfosDF.rdd.map(row => (row.getAs[String]("name"), row.getAs[Int]("age")))
                .join(
                    stuentsScoresDF.rdd.map(row => (row.getAs[String]("name"), row.getAs[Int]("score")))
                )
                
    
        val studentRowsRDD = studentsRDD.map(tuple => Row(tuple._1, tuple._2._1, tuple._2._2))
        
        val structType = StructType(Array(
            StructField("name", StringType, true),
            StructField("score", IntegerType, true),
            StructField("age", IntegerType, true)))
        
        val studentsDF = sparkSession.createDataFrame(studentRowsRDD, structType)
        
        // 将DataFrame中的数据保存到mysql中
        val studentsDFOptions = mutable.Map("url" -> "jdbc:mysql://localhost:3306/spark_sql",
            "user" -> "root",
            "password" -> "542919899",
            "dbtable" -> "students_df")
        
        // 2.1.0的版本支持？？？https://issues.apache.org/jira/browse/SPARK-14525
//        studentsDF.write.format("jdbc").options(studentsDFOptions).save
    
        studentRowsRDD.foreach{ x =>
            {
                println(x.getString(0) + " " + x.getInt(1) + " " + x.getInt(2))
                
                val sql = "insert into students_df values (" +
                    "'" + x.getString(0) + "'," +
                    "'" + x.getInt(1) + "'," +
                    "'" + x.getInt(2) + "')";
                
                println(sql)
                
                Class.forName("com.mysql.jdbc.Driver")
                val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_sql", "root", "542919899");
                val stmt =  conn.createStatement
                stmt.executeUpdate(sql)
                
                stmt.close()
                conn.close()
            }
        }
    }
}
