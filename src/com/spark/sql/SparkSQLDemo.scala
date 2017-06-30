package com.spark.sql

import org.apache.spark.sql.{Row, SparkSession}


/**
  * Created by yangtong on 17/6/30.
  */
object SparkSQLDemo {
    // 使用DataSet，通常是通过case class来定义DataSet对象
    case class Person(name: String, age: Long)
    
    case class Record(key: Int, value: String)
    
    
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
                .builder()
                .master("local")
                .appName("SparkSQLDemo")
                .enableHiveSupport()
                .getOrCreate()
        
        import spark.implicits._
        
//        val df = spark.read.json("./data/sql/people.json")
//        df.show()
//        df.printSchema()
//        df.select("name").show()
//        df.select($"name", $"age"+1).show
//        df.filter($"age" > 21).show()
//        df.groupBy("age").count().show()
//        df.createOrReplaceTempView("people")
        
        
//        val sqlDF = spark.sql("SELECT * FROM people")
//        sqlDF.show()
    
        // 基于jvm object构造DataSet
//        val caseClassDS = Seq(Person("Andy", 32)).toDS()
//        caseClassDS.show()
//
//        // 基于原始数据类型构造DataSet
//        val primitiveDS = Seq(1, 2, 3).toDS()
//        primitiveDS.show()
//
//        // 基于已有数据文件来构造DataSet
//        val path = "./data/sql/people.json"
//        val peopleDS = spark.read.json(path).as[Person] // 首先得到是一个DataFrame，然后使用as[Person]
//        peopleDS.show()
    
    
        // hive 操作 要开启enableHiveSupport
        spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
        spark.sql("LOAD DATA LOCAL INPATH './data/sql/kv1.txt' INTO TABLE src")
        spark.sql("SELECT * FROM src").show()
        spark.sql("SELECT COUNT(*) FROM src").show()
    
        val sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")
        val stringsDS = sqlDF.map {
            case Row(key: Int, value: String) => s"Key: $key, Value: $value"
        }
        stringsDS.show()
    
        val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
        recordsDF.createOrReplaceTempView("records")
        spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()
    
    
    }
}
