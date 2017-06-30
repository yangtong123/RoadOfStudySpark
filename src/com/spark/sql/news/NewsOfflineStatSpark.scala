package com.spark.sql.news

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession

/**
  * 新闻网站关键指标离线统计spark作业
  * Created by yangtong on 17/6/29.
  */
class NewsOfflineStatSpark {
    /**
      * 获取昨天字符串类型日期
      * @return
      */
    def getYesterday(): String = {
        val cal = Calendar.getInstance()
        cal.setTime(new Date())
        cal.add(Calendar.DAY_OF_YEAR, -1)
        
        val yesterday = cal.getTime
        
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val date = sdf.format(yesterday)
        date
    }
    
    /**
      * 计算每天每个页面的pv，并排序
      * @return
      */
    def calaculateDailyPagePV(sparkSession: SparkSession, date: String) {
        val sql =
            "SELECT " +
                "date, " +
                "pageId, " +
                "pv " +
            "FROM ( " +
                "SELECT " +
                    "date, " +
                    "pageId, " +
                    "count(*) pv " +
                "FROM news_access " +
                "WHERE action='view' " +
                "AND date='" + date + "' " +
                "GROUP BY date, pageId " +
            ") t " +
            "ORDER BY pv DESC "
        
        val df = sparkSession.sql("sql")
        
        df.show()
    }
    
    /**
      * 计算每天每个页面的uv，并排序
      * @return
      */
    def calculateDailyPageUV(sparkSession: SparkSession, date: String) {
        val sql =
            "SELECT " +
                "date, " +
                "pageId, " +
                "uv " +
            "FROM ( " +
                "SELECT " +
                    "date, " +
                    "pageId, " +
                    "count(*) uv" +
                "FROM ( " +
                    "SELECT " +
                        "date, " +
                        "pageId, " +
                        "userId " +
                    "FROM news_access " +
                    "WHERE action='view' " +
                    "AND date='" + date + "' " +
                    "GROUP BY date, pageId, userId " +
                ") t2 " +
                "GROUP BY date, pageId " +
            ") t " +
            "ORDER BY uv DESC "
        
        val df = sparkSession.sql(sql)
        df.show()
    }
    
    /**
      * 新用户注册比率统计
      * @param sparkSession
      * @param date
      */
    def calculateDailyNewUserRegisterRate(sparkSession: SparkSession, date: String): Unit = {
        // userId为null的访问总数
        val sql1 = "SELECT count(*) FROM news_access WHERE action='view' AND date='" + date + "' AND userId is NULL"
        // 昨天的总注册用户数
        val sql2 = "SELECT count(*) FROM news_access WHERE action='register' AND date='" + date + "' "
        
        // 执行两条sql，获取结果
        val result1 = sparkSession.sql(sql1).collect()(0).get(0)
        var number1 = 0L
        if (result1 != null) {
            number1 = result1.toString.toLong
        }
    
        val result2 = sparkSession.sql(sql2).collect()(0).get(0)
        var number2 = 0L
        if (result2 != null) {
            number2 = result2.toString.toLong
        }
        
        // 计算结果
        val rate = number2.toDouble / number1.toDouble;
        rate.formatted("%.2f")
    }
    
    /**
      * 计算每天的用户跳出率
      * @param sparkSession
      * @param date
      */
    def calculateDailyUserJumpRate(sparkSession: SparkSession, date: String): Unit = {
        // 计算已注册用户的昨天的总的访问pv
        val sql1 = "SELECT count(*) FROM news_access WHERE action='view' AND date='" + date + "' AND userId IS NOT NULL "
        // 已注册用户的昨天跳出的总数
        val sql2 = "SELECT count(*) FROM ( SELECT count(*) cnt FROM news_access WHERE action='view' AND date='" + date + "' AND userId IS NOT NULL GROUP BY userId HAVING cnt=1 ) t "
    
        // 执行两条sql，获取结果
        val result1 = sparkSession.sql(sql1).collect()(0).get(0)
        var number1 = 0L
        if (result1 != null) {
            number1 = result1.toString.toLong
        }
    
        val result2 = sparkSession.sql(sql2).collect()(0).get(0)
        var number2 = 0L
        if (result2 != null) {
            number2 = result2.toString.toLong
        }
    
        // 计算结果
        val rate = number2.toDouble / number1.toDouble;
        rate.formatted("%.2f")
        
    }
    
    /**
      * 计算每天板块热度排行榜
      * @param sparkSession
      * @param date
      */
    def calculateDailySectionPVSort(sparkSession: SparkSession, date: String): Unit = {
        val sql =
            "SELECT " +
                "date, " +
                "section, " +
                "pv " +
            "FROM ( " +
                "SELECT " +
                    "date, " +
                    "section, " +
                    "count(*) pv " +
                "FROM news_access " +
                "WHERE action = 'view' " +
                "AND date='" + date + "' " +
                "GROUP BY date, section " +
            ") t " +
            "ORDER BY pv DESC "
        
        val df = sparkSession.sql(sql)
        df.show()
    }
}

object NewsOfflineStatSpark {
    def main(args: Array[String]): Unit = {
        val newsOfflineStat = new NewsOfflineStatSpark()
        
        val sparkSession = SparkSession.builder()
                .appName("NewsOfflineStatSpark")
                .master("local[2]")
                .enableHiveSupport()
                .getOrCreate()
        
        val yesterday = newsOfflineStat.getYesterday()
    
        // 页面pv统计以及排序
        newsOfflineStat.calaculateDailyPagePV(sparkSession, yesterday)
    
        // 开发第二个关键指标：页面uv统计以及排序
        newsOfflineStat.calculateDailyPageUV(sparkSession, yesterday);
        // 开发第三个关键指标：新用户注册比率统计
        newsOfflineStat.calculateDailyNewUserRegisterRate(sparkSession, yesterday);
        // 开发第四个关键指标：用户跳出率统计
        newsOfflineStat.calculateDailyUserJumpRate(sparkSession, yesterday);
        // 开发第五个关键指标：版块热度排行榜
        newsOfflineStat.calculateDailySectionPVSort(sparkSession, yesterday);
    }
    
   
}
