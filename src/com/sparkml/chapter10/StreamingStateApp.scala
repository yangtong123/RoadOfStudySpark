package com.sparkml.chapter10

import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by yangtong on 17/4/19.
  */
object StreamingStateApp {

  /*
      首先定义一个updateState函数来基于运行状态值和新的当前批次数据计算新状态。
      状态在这种情况下是一个"(产品数量, 营收)"元祖，针对每个用户。
      给定当前时刻的当前批次和累积状态的"(产品，收入)"对的集合，计算得到新的状态
   */
  def updateState(prices: Seq[(String, Double)], currentTotal: Option[(Int, Double)]) = {
    val currentRevenue = prices.map(_._2).sum
    val currentNumberPurchases = prices.size
    val state = currentTotal.getOrElse((0, 0.0))
    Some((currentNumberPurchases + state._1, currentRevenue + state._2))
  }

  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext("local[2]", "First Streaming App", Seconds(10))
    //对有状态的操作需要设置一个检查点
    ssc.checkpoint("./sparkstreaming")
    val stream = ssc.socketTextStream("localhost", 9999)

    val events = stream.map { record =>
      val event = record.split(",")
      (event(0), event(1), event(2).toDouble)
    }

    val users = events.map { case (user, product, price) =>
      (user, (product, price))
    }
    val revenuePerUser = users.updateStateByKey(updateState)
    revenuePerUser.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
