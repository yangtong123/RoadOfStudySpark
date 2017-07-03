package com.spark.sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by yangtong on 17/6/16.
  */
object UDF {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession.builder()
                .appName("UDF")
                .master("local[2]")
                .getOrCreate()
        
        val names = Array("zhangsan", "lisi", "wangwu", "Tom", "Jerry")
        val namesRDD = sparkSession.sparkContext.parallelize(names)
        val namesRowRDD = namesRDD.map(x => Row(x))
        val schema = StructType(Array(
            StructField("name", StringType, true)
        ))
        val namesDF = sparkSession.createDataFrame(namesRowRDD, schema)
        
        namesDF.createTempView("names")
        
        sparkSession.udf.register("strlen", (str: String) => str.length)
        
        sparkSession.sql("select name, strlen(name) len from names").show()
    }
}
