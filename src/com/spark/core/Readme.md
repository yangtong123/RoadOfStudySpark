# Spark Core学习笔记

## 一、各个算子的学习笔记

### 1.1 map mapPartition mapPartitionWithIndex
map和mapPartition的区别就是：  
map一次处理一个partition中的一条数据，mapPartition一次处理一个partition的全部数据  
mapPartitionWithIndex可以得到每个partition的index，从0开始，用法参见[MapPartitionWithIndex](./MapPartitionWithIndex.scala)

### 1.2 sample
sample算子就是从数据集中抽取一部分数据，第一个参数是是否替换抽取出来的数据，第二个参数是抽取的百分比，第三个参数是用来生成随机数的种子

### 1.3 union
union操作就是把两个RDD聚合成一个RDD，其中是原封不动的把各个RDD的partition复制到新RDD中去

### 1.4 groupByKey

