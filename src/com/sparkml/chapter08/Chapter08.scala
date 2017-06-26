package com.sparkml.chapter08

import java.awt.image.BufferedImage
import java.io.File
import javax.imageio.ImageIO

import breeze.linalg.DenseMatrix
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangtong on 17/4/17.
  */
object Chapter08 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Chapter08").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val path = "./data/lfw/*"
    val rdd = sc.wholeTextFiles(path)
    val files = rdd.map { case (fileName, content) =>
      fileName.replace("file:", "")
    }
//    println(files.first())

    /**
      * 1.提取面部图片作为向量
      */
    //(1)载入图片
    def loadImageFromFile(path: String): BufferedImage = {
      ImageIO.read(new File(path))
    }
    //(2)转换灰度照片并改变图片尺寸
    def processImage(image: BufferedImage, width: Int, height: Int): BufferedImage = {
      val bwImage = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY)
      val g = bwImage.getGraphics
      g.drawImage(image, 0, 0, width, height, null)
      g.dispose()
      bwImage
    }
    //(3)提取特征向量
    def getPixelsFromImage(image: BufferedImage): Array[Double] = {
      val width = image.getWidth()
      val height = image.getHeight()
      val pixels = Array.ofDim[Double](width * height)
      image.getData().getPixels(0, 0, width, height, pixels)
    }

    def extractPixels(path: String, width: Int, height: Int): Array[Double] = {
      val raw = loadImageFromFile(path)
      val processed = processImage(raw, width, height)
      getPixelsFromImage(processed)
    }

    val pixels = files.map(f => extractPixels(f, 50, 50))
    println(pixels.take(10).map(_.take(10).mkString("", ",",", ...")).mkString("\n"))

    //为每张图片创建MLib向量对象
    val vectors: RDD[Vector] = pixels.map(p => Vectors.dense(p))
    vectors.setName("image-vectors")
    vectors.cache()

    /**
      * 2.正则化
      */
    val scaler = new StandardScaler(withMean = true, withStd = false).fit(vectors)
    val scaledVectors = vectors.map(v => scaler.transform(v))


    /**
      * 3.训练降维模型
      */
    val matrix = new RowMatrix(scaledVectors)
    val K = 10
    val pc = matrix.computePrincipalComponents(K)

    val rows = pc.numRows
    val cols = pc.numCols

    val pcBreeze = new DenseMatrix(rows, cols, pc.toArray)
    breeze.linalg.csvwrite(new File("./data/tmp/pc.csv"), pcBreeze)

    /**
      * 4.使用降维模型
      */
    val projected = matrix.multiply(pc)

    //PCA和SVD模型之间的关系
    val svd = matrix.computeSVD(10, computeU = true)
    println(s"U dimension: (${svd.U.numRows}, ${svd.U.numCols})")
    println(s"S dimension: (${svd.s.size}, )")
    println(s"V dimension: (${svd.V.numRows}, ${svd.V.numCols})") //矩阵V和PCA的结果完全一样

    //比较是否相等
    def approxEqual(array1: Array[Double], array2: Array[Double], tolerance: Double = 1e-6): Boolean = {
      //没有考虑符号
      val bools = array1.zip(array2).map { case (v1, v2) =>
        if (math.abs(math.abs(v1) - math.abs(v2)) > 1e-6) false else true
      }
      bools.fold(true)(_ & _)
    }

    println(approxEqual(svd.V.toArray, pc.toArray))

    //矩阵U和向量S的乘积和PCA中用来把原始图像数据投影到10个主成分构成的空间中的投影矩阵相等：
    val breezeS = breeze.linalg.DenseVector(svd.s.toArray)
    val projectedSVD = svd.U.rows.map { v =>
      val breezeV = breeze.linalg.DenseVector(v.toArray)
      val multV = breezeV :* breezeS  //:*表示对向量执行对应元素和元素的乘法
      Vectors.dense(multV.data)
    }
    projected.rows.zip(projectedSVD).map {case (v1, v2) =>
      approxEqual(v1.toArray, v2.toArray)
    }.filter(b => true).count()


    /**
      * 5.评价降维模型
      */
    val svd300 = matrix.computeSVD(300, computeU = false)
    val sMatrix = new DenseMatrix(1, 300, svd300.s.toArray)
    breeze.linalg.csvwrite(new File("./data/tmp/s.csv"), sMatrix)
  }
}
