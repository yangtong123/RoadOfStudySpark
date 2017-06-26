package com.spark.sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by yangtong on 17/6/16.
  */
object UDAF {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession.builder()
                .appName("UDAF")
                .master("local[2]")
                .getOrCreate()
        
        val names = Array("zhangsan", "lisi", "wangwu", "Tom", "Jerry", "zhangsan", "Tom")
        val namesRDD = sparkSession.sparkContext.parallelize(names)
        val namesRowRDD = namesRDD.map(x => Row(x))
        val schema = StructType(Array(
            StructField("name", StringType, true)
        ))
        val namesDF = sparkSession.createDataFrame(namesRowRDD, schema)
        
        namesDF.createTempView("names")
        
        sparkSession.udf.register("strCount", new UDAFStringCount)
        
        sparkSession.sql("select name, strCount(name) len from names group by name").show()
    }
}
