package com.spark.sql.news

import java.io.{FileOutputStream, OutputStreamWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import scala.util.Random

/**
  * 离线数据生成
  *
  * Created by yangtong on 17/6/29.
  */
object OfflineDataGenerator {
    def main(args: Array[String]): Unit = {
        val buffer = new StringBuilder("")
        
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val random = new Random
        val sections = Array[String]("country", "international", "sport", "entertainment", "movie", "carton", "tv-show", "technology", "internet", "car")
        val newOldUserArr = Array[Int](1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        
        // 生成日期，默认就是昨天
        val cal = Calendar.getInstance()
        cal.setTime(new Date())
        cal.add(Calendar.DAY_OF_YEAR, -1)
        val yesterday = cal.getTime
        
        val date = sdf.format(yesterday)
        
        // 生成1000条访问数据
        for (i <- 0 until 1000) {
            // 生成时间戳
            val timestamp = new Date().getTime
            // 生成随机userid(默认1000注册用户， 每天1/10的访客是未注册用户)
            var userId: String = null
            val newOldUser = newOldUserArr(random.nextInt(10))
            userId = if (newOldUser == 1) null else String.valueOf(random.nextInt(1000))
            // 生成随机的pageId
            val pageId = random.nextInt(1000)
            // 生成随机模块
            val section = sections(random.nextInt(10))
            // 生成固定行为view
            val action = "view"
            
            buffer.append(date).append("\t")
                    .append(timestamp).append("\t")
                    .append(userId).append("\t")
                    .append(pageId).append("\t")
                    .append(section).append("\t")
                    .append(action).append("\n")
        }
        
        // 生成10条注册数据
        for (i <- 0 until 10) {
            // 生成时间戳
            val timestamp = new Date().getTime
            // 新用户都是userId为null
            val userId: String = null
            val pageId: String = null
            val section: String = null
            val action = "register"
            
            buffer.append(date).append("\t")
                    .append(timestamp).append("\t")
                    .append(userId).append("\t")
                    .append(pageId).append("\t")
                    .append(section).append("\t")
                    .append(action).append("\n")
        }
    
        // 输出到文件
        
        var pw: PrintWriter = null
        try {
            pw = new PrintWriter(new OutputStreamWriter(
                new FileOutputStream("/Users/yangtong/Desktop/access.log")))
            pw.write(buffer.toString)
        } catch {
            case e: Exception => e.printStackTrace()
        } finally {
            pw.close()
        }
    }
}
