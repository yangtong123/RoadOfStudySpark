package com.sparkml.chapter10

import java.io.PrintWriter
import java.net.ServerSocket

import scala.util.Random

/**
  * Created by yangtong on 17/4/19.
  */
object Chapter10 {

}

object StreamingProducer {
  def main(args: Array[String]): Unit = {
    val random = new Random()

    //每秒最大活动数
    val MaxEvents = 6

    //读取可能的名称
    val namesResource = this.getClass.getResourceAsStream("name.csv")
    val names = scala.io.Source.fromInputStream(namesResource)
      .getLines()
      .toList
      .head
      .split(",")
      .toSeq

    //生成一系列可能的产品
    val products = Seq (
      "iPhone Cover" -> 9.99,
      "Headphones" -> 5.49,
      "Samsung Galaxy Cover" -> 8.95,
      "iPad Cover" -> 7.49
    )

    //生成随机产品活动
    def generateProductEvents(n: Int) = {
      (1 to n).map { i =>
        val (product, price) = products(random.nextInt(products.size))
        val user = random.shuffle(names).head
        (user, product, price)
      }
    }

    //创建网络生成器
    val listener = new ServerSocket(9999)
    println("Listening on port: 9999")

    while (true) {
      val socket = listener.accept()
      new Thread() {
        override def run() = {
          println("Got client connected from: " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream, true)

          while (true) {
            Thread.sleep(1000)
            val num = random.nextInt(MaxEvents)
            val productEvents = generateProductEvents(num)
            productEvents.foreach { event =>
              out.write(event.productIterator.mkString(","))
              out.write("\n")
            }
            out.flush()
            println(s"Created $num events...")
          }
          socket.close()
        }
      }.start()
    }
  }


}
