package com.spark.streaming

import java.sql.{Connection, DriverManager}
import java.util

/**
  * Created by yangtong on 17/6/19.
  */
object ConnectionPool {
    var connectionQueue: util.LinkedList[Connection] = null
    
    try {
        Class.forName("com.mysql.jdbc.Driver")
    } catch {
        case e: Exception => {
            e.printStackTrace()
        }
    }
    
    // 获取连接，多线程访问并发控制
    def getConnection(): Connection = {
        this.synchronized {
            try {
                if (connectionQueue == null) {
                    connectionQueue = new util.LinkedList[Connection]
                    for (i <- 0 to 10) {
                        val conn: Connection = DriverManager.getConnection(
                            "jdbc:mysql://localhost:3306", "root", "542919899")
                        connectionQueue.push(conn)
                    }
                }
            } catch {
                case e: Exception => {
                    e.printStackTrace()
                }
            }
            connectionQueue.poll()
        }
    }
    
    
    def returnConnection(conn: Connection): Unit = {
        
    }
    
}
