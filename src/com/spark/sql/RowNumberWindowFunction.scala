package com.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by yangtong on 17/6/15.
  */
object RowNumberWindowFunction {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession.builder()
                .appName("RowNumberWindowFunction")
                .master("local[2]")
                .enableHiveSupport()
                .getOrCreate()
        
        sparkSession.sql("drop table if exists sales")
        
        sparkSession.sql("create table if not exists sales (" +
                "product String, " +
                "category String, " +
                "revenue BigInt" +
                ") " +
                "row format delimited fields terminated by '\t'")
        
        sparkSession.sql("load data local inpath './data/sql/sales.txt' overwrite into table sales")
        
        sparkSession.sql("select * from sales").show()
        
        // row_number()的使用
        // 给每个分组的数据，按照其排序顺序，搭上一个分组内的行号
        // 比如说，又一个分组data=20151001，里面有3条数据,1122,1121,1124
        // 那么对这个分组的每一行使用row_number()开窗函数以后，三行，依次会获得一个组内的行号
        // 行号从1开始递增，比如1122 1，1121 2，1124 3
        val top3SalesDF = sparkSession.sql(
            "select product,category,revenue " +
            "from (" +
                "select " +
                    "product," +
                    "category," +
                    "revenue, " +
                    "row_number() over (partition by category order by revenue desc) rank " +
                "from sales " +
            ") tmp_sales " +
            "where rank <= 3"
        )
        
        top3SalesDF.show()
        
        
    }
}
