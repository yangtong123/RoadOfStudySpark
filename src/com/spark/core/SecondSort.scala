package com.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangtong on 17/6/22.
  */
object SecondSort {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
                .setAppName("SecondSort")
                .setMaster("local[2]")
        val sc = new SparkContext(conf)
    
        val lines = sc.textFile("./data/core/sort.txt")
    
        val pairs = lines.map { line =>
            (new SecondSortKey(line.split(" ")(0).toInt, line.split(" ")(1).toInt), line)
        }
        
        val sortedPairs = pairs.sortByKey()
        
        val sortedLines = sortedPairs.map(sortedPair => sortedPair._2)
    
        sortedLines.foreach(println)
    }
}

class SecondSortKey(val first: Int, val second : Int) extends Ordered[SecondSortKey] with Serializable {
    override def compare(that: SecondSortKey): Int = {
        if (this.first - that.first != 0) {
            this.first - that.first
        } else {
            this.second - that.second
        }
    }
}
