# Spark Core学习笔记

## 各个算子的学习笔记

### map mapPartition mapPartitionWithIndex
map和mapPartition的区别就是  
map一次处理一个partition中的一条数据，mapPartition一次处理一个partition的全部数据  
mapPartitionWithIndex可以得到每个partition的index，从0开始，用法参见[MapPartitionWithIndex](./MapPartitionWithIndex.scala)