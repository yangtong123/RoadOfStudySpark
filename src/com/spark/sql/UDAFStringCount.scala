package com.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * Created by yangtong on 17/6/16.
  */
class UDAFStringCount extends UserDefinedAggregateFunction {
    // 输入数据的类型
    override def inputSchema: StructType = {
        StructType(Array(StructField("str", StringType, true)))
    }
    // 中间聚合时所处理的数据
    override def bufferSchema: StructType = {
        StructType(Array(StructField("count", IntegerType, true)))
    }
    // 函数返回的类型
    override def dataType: DataType = {
        IntegerType
    }
    // 指定是否是确定性的
    override def deterministic: Boolean = {
        true
    }
    
    /**
      * 为每个分组的数据执行初始化操作
      * @param buffer
      */
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = 0
    }
    // 每个分组有新值过来，如何进行分组对应的聚合值的计算
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        buffer(0) = buffer.getAs[Int](0) + 1
    }
    // 合并，一个分组的数据会分布在多个节点上处理，所以最后要用merge
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
    }
    // 通过中间的缓存聚合值，最后返回一个最终的聚合值
    override def evaluate(buffer: Row): Any = {
        buffer.getAs[Int](0)
    }
}
