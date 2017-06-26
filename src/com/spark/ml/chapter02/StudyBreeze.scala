package com.spark.ml.chapter02

import breeze.linalg._
import breeze.numerics._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Breeze的相关操作
  * Created by yangtong on 17/6/5.
  */
object StudyBreeze {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession
                .builder()
                .appName("breeze_test01")
                .master("local[2]")
                .getOrCreate();
        
        Logger.getRootLogger.setLevel(Level.WARN)
        
        // 3.1.1 Breeze创建函数
        val m1 = DenseMatrix.zeros[Double](2, 3)
        val v1 = DenseVector.zeros[Double](3)
        val v2 = DenseVector.ones[Double](3)
        val v3 = DenseVector.fill(3) {5.0}
        val v4 = DenseVector.range(1, 10, 2) // 生成随机向量 start stop step
        val m2 = DenseMatrix.eye[Double](3)  // 单位矩阵
        val v6 = diag(DenseVector(1.0, 2.0, 3.0)) // 对角矩阵
        val m3 = DenseMatrix((1.0, 2.0), (3.0, 4.0))
        val v8 = DenseVector(1, 2, 3, 4)
        val v9 = DenseVector(1, 2, 3, 4).t
        val v10 = DenseVector.tabulate(3) {i => 2 * i} // 从函数创建向量
        val m4 = DenseMatrix.tabulate(3, 2) {case (i, j) => i + j} // 从函数创建矩阵
        val v11 = new DenseVector(Array(1, 2, 3, 4))
        val m5 = new DenseMatrix(2, 3, Array(11, 12, 13, 21, 22, 23))
        val v12 = DenseVector.rand(4)  // 0到1的随机向量
        val m6 = DenseMatrix.rand(2, 3) // 0到1的随机向量
        
        // 3.1.2 Breeze 元素访问及操作函数
        // 元素访问
        val a = DenseVector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        a(0)
        a(1 to 4) // a(1 until 5) a.slice(1, 5)
        a(5 to 0 by -1) // 按照指定步长取子集
        a(1 to -1)
        a(-1)
        val m = DenseMatrix((1.0, 2.0, 3.0), (3.0, 4.0, 5.0))
        m(0, 1)
        m(::, 1)  // 矩阵指定列
        
        // 元素操作
        val m_1 = DenseMatrix((1.0, 2.0, 3.0), (3.0, 4.0, 5.0))
        m_1.reshape(3, 2) // 调整矩阵形状
        m_1.toDenseVector
        
        val m_3 = DenseMatrix((1.0, 2.0, 3.0), (4.0, 5.0, 6.0), (7.0, 8.0, 9.0))
        lowerTriangular(m_3)
        upperTriangular(m_3)
        m_3.copy
        diag(m_3)
        m_3(::, 2) := 5.0
        m_3
        m_3(1 to 2, 1 to 2) := 5.0
        m_3
        
        val a_1 = DenseVector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        a_1(1 to 4) := 5
        a_1(1 to 4) := DenseVector(1, 2, 3, 4)
        a_1
        val a1 = DenseMatrix((1.0, 2.0, 3.0), (4.0, 5.0, 6.0))
        val a2 = DenseMatrix((1.0, 1.0, 1.0), (2.0, 2.0, 2.0))
        DenseMatrix.vertcat(a1, a2)
        DenseMatrix.horzcat(a1, a2)
        val b1 = DenseVector(1, 2, 3, 4)
        val b2 = DenseVector(1, 1, 1, 1)
        DenseVector.vertcat(b1, b2)
    
        // 3.1.3 Breeze 数值计算函数
        val a_3 = DenseMatrix((1.0, 2.0, 3.0), (4.0, 5.0, 6.0))
        val b_3 = DenseMatrix((1.0, 1.0, 1.0), (2.0, 2.0, 2.0))
        a_3 + b_3
        a_3 :* b_3
        a_3 :/ b_3
        a_3 :< b_3
        a_3 :== b_3
        a_3 :+= 1.0
        a_3 :*= 2.0
        max(a_3)
        argmax(a_3)
        DenseVector(1, 2, 3, 4) dot DenseVector(1, 1, 1, 1)
    
        // 3.1.4 Breeze 求和函数
        val a_4 = DenseMatrix((1.0, 2.0, 3.0), (4.0, 5.0, 6.0), (7.0, 8.0, 9.0))
        sum(a_4)
        sum(a_4, Axis._0) // 对列求和
        sum(a_4, Axis._1) // 对行求和
        trace(a_4)
        accumulate(DenseVector(1, 2, 3, 4))
    
        // 3.1.5 Breeze 布尔函数
        val a_5 = DenseVector(true, false, true)
        val b_5 = DenseVector(false, true, true)
        a_5 :& b_5
        a_5 :| b_5
        !a_5
        val a_5_2 = DenseVector(1.0, 0.0, -2.0)
        any(a_5_2) // 任意元素非0
        all(a_5_2) // 所有元素非0
    
        // 3.1.6 Breeze 线性代数函数
        val a_6 = DenseMatrix((1.0, 2.0, 3.0), (4.0, 5.0, 6.0), (7.0, 8.0, 9.0))
        val b_6 = DenseMatrix((1.0, 1.0, 1.0), (1.0, 1.0, 1.0), (1.0, 1.0, 1.0))
        a_6 \ b_6
        a_6.t
        det(a_6)
        inv(a_6)
        val svd.SVD(u, s, v) = svd(a_6)
        a_6.rows
        a_6.cols
    
        // 3.1.7 Breeze 取整函数
        val a_7 = DenseVector(1.2, 0.6, -2.3)
        round(a_7)
        ceil(a_7)
        floor(a_7)
        signum(a_7)
        abs(a_7)
    }
}
