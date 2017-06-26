package com.sparkml.chapter10

import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by yangtong on 17/4/19.
  */
object SimpleStreamingApp {
  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext("local[2]", "First Streaming App", Seconds(10))
    val stream = ssc.socketTextStream("localhost", 9999)

    stream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
