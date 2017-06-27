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
groupByKey等shuffle算子，都会创建一些隐式RDD，主要是作为这个操作的一些中间数据的表达，以及作为stage划分的边界。如下图的shuffleRDD，作为一个shuffle过程中的中间数据代表，
依赖这个shuffleRDD创建出来一个新的stage(stage1)，ShuffleRDD会触发shuffle read操作。从上游stage的task所在节点，拉取过来相同的key，做进一步聚合。
对shuffleRDD中的数据执行一个map类操作，主要是对每个partition中的数据，都进行一个映射喝聚合。这里主要是将每个key对应的数据都聚合到一个Iterator集合中。
<div align=center>
    <img src="./pic/groupByKey.png" width="70%" height="50%"/>
</div>

### 1.5 reduceByKey
reduceByKey和groupByKey异同之处</br>
> 1. 不同之处：reduceByKey,多了一个RDD，MapPartitionRDD，存在于stage0的，主要是代表了进行本地数据规约之后的rdd，
所以，网络传输的数据量以及磁盘I/O等都会减少，性能更高。</br>
> 2. 相同之处: 后面进行shuffle read和聚合的过程基本喝groupByKey类似。都是shuffleRDD，去做shuffle read。然后聚合，
聚合后的数据就是最终的RDD。</br>
<div align=center>
    <img src="./pic/reduceByKey.png" width="70%", height="50%"/>
</div>

### 1.6 distinct
distinct的原理：</br>
> 1. 首先map操作给自己每个值都打上一个v2，变成一个tuple</br>
> 2. 然后调用reduceByKey(仅仅返回一个value) </br>
> 3. 将去重后的数据，从tuple还原为单值</br>
<div align=center>
    <img src="./pic/distinct.png" width="70%", height="50%"/>
</div>

### 1.7 cogroup
把多个RDD中的数据根据key聚合起来
<div align=center>
    <img src="./pic/cogroup.png" width="70%", height="50%"/>
</div>

### 1.8 intersection
intersection的原理：
> 1. 首先map操作变成一个tuple
> 2. 然后cogroup聚合两个RDD的key
> 3. filter, 过滤掉两个集合中任意一个集合为空的key
> 4. map，还原出单key


