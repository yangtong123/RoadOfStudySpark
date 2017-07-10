# Spark机器学习笔记

<script type="text/javascript" src="http://cdn.mathjax.org/mathjax/latest/MathJax.js?config=default"></script>

## Spark MLlib矩阵向量
Spark MLlib底层的向量、矩阵运算使用了Breeze库,Breeze库提供了Vector/Matrix的实现以及 相应计算的接口(Linalg)。但是在MLlib里面同时也提供了Vector和Linalg等的实现。  
在使用Breeze 库时,需要导入相关包:  
``` scala
import breeze.linalg._  
import breeze.numerics._
```
### Breeze创建函数
<div align=center>
    <img src="./pic/Breeze创建函数.png">
</div>

主要实现代码见[StudyBreeze](./chapter02/StudyBreeze.scala)

## 线性回归

$$x=\frac{-b\pm\sqrt{b^2-4ac}}{2a}$$
\\(x=\frac{-b\pm\sqrt{b^2-4ac}}{2a}\\)