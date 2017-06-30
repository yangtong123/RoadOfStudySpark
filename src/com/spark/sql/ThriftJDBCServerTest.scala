package com.spark.sql

import java.sql.DriverManager

/**
  * Created by yangtong on 17/6/29.
  */
object ThriftJDBCServerTest {
    def main(args: Array[String]): Unit = {
        Class.forName("org.apache.hive.jdbc.HiveDriver")
        val conn = DriverManager.getConnection("jdbc:hive2://spark01:10000/", "root", "")
        
        try {
            val statement = conn.createStatement
            statement.executeQuery("use hive")
            val rs = statement.executeQuery("select * from ... where ...")
    
            while (rs.next()) {
                val ordernumber = rs.getString("ordernumber")
                val amount = rs.getString("amount")
                println("ordernumber = %s, amount = %s".format(ordernumber, amount))
            }
        } catch {
            case e: Exception => e.printStackTrace
        }
    }
}
