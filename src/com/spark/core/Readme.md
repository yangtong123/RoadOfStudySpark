# Spark Core学习笔记

<p>
<a href="#一各个算子学习笔记">一、各个算子的学习笔记</a></br>
    <a href="#map-mapPartitions-mapPartitionWithIndex">map mapPartitions mapPartitionWithIndex</a></br>
    <a href=""></a></br>
    <a href=""></a></br>


</p>

## 一、各个算子的学习笔记

### map mapPartitions mapPartitionWithIndex
map和mapPartitions的区别就是：  
map一次处理一个partition中的一条数据，mapPartitions一次处理一个partition的全部数据  
mapPartitionWithIndex可以得到每个partition的index，从0开始，用法参见[MapPartitionWithIndex](./MapPartitionWithIndex.scala)

### sample和takeSample
sample算子就是从数据集中抽取一部分数据，第一个参数是是否替换抽取出来的数据，第二个参数是抽取的百分比，第三个参数是用来生成随机数的种子, [Sample](./Sample.scala)</br>
takeSample与sample不同之处
> 1. action操作，sample是transformation操作
> 2. 不能指定抽取比例，只能是抽取几个
``` scala
val luckyBoy = staffRDD.takeSample(false, 3)
```

### union
union操作就是把两个RDD聚合成一个RDD，其中是原封不动的把各个RDD的partition复制到新RDD中去
``` scala
val department1StaffList = List("张三", "李四", "王二", "麻子") 
val department1StaffRDD = sc.parallelize(department1StaffList)
        
val department2StaffList = List("赵六", "王五", "小明", "小倩")
val department2StaffRDD = sc.parallelize(department2StaffList)
        
val departmentStaffRDD = department1StaffRDD.union(department2StaffRDD)
```

### groupByKey
>groupByKey等shuffle算子，都会创建一些隐式RDD，主要是作为这个操作的一些中间数据的表达，以及作为stage划分的边界。如下图的shuffleRDD，作为一个shuffle过程中的中间数据代表，
依赖这个shuffleRDD创建出来一个新的stage(stage1)，ShuffleRDD会触发shuffle read操作。从上游stage的task所在节点，拉取过来相同的key，做进一步聚合。
对shuffleRDD中的数据执行一个map类操作，主要是对每个partition中的数据，都进行一个映射喝聚合。这里主要是将每个key对应的数据都聚合到一个Iterator集合中。
<div align=center>
    <img src="./pic/groupByKey.png" width="70%" height="50%"/>
</div>

### reduceByKey
reduceByKey和groupByKey异同之处</br>
> 1. 不同之处：reduceByKey,多了一个RDD，MapPartitionRDD，存在于stage0的，主要是代表了进行本地数据规约之后的rdd，
所以，网络传输的数据量以及磁盘I/O等都会减少，性能更高。</br>
> 2. 相同之处: 后面进行shuffle read和聚合的过程基本喝groupByKey类似。都是shuffleRDD，去做shuffle read。然后聚合，
聚合后的数据就是最终的RDD。</br>
<div align=center>
    <img src="./pic/reduceByKey.png" width="70%", height="50%"/>
</div>

### aggregateByKey
> reduceByKey认为是aggregateByKey的简化版
  aggregateByKey最重要的一点是，多提供了一个函数，Seq Function
  就是说自己可以控制如何对每个partition中的数据进行先聚合，类似于mapreduce中的，map-side combine
  然后才是对所有partition中的数据进行全局聚合</br>
>> * 第一个参数是，每个key的初始值
>> * 第二个是个函数，Seq Function，如何进行shuffle map-side的本地聚合
>> * 第三个是个函数，Combiner Function，如何进行shuffle reduce-side的全局聚合
[AggregateByKey示例代码](./AggregateByKey)


### distinct
distinct的原理：</br>
> 1. 首先map操作给自己每个值都打上一个v2，变成一个tuple</br>
> 2. 然后调用reduceByKey(仅仅返回一个value) </br>
> 3. 将去重后的数据，从tuple还原为单值</br>
<div align=center>
    <img src="./pic/distinct.png" width="70%", height="50%"/>
</div>

### cogroup
cogroup的原理：</br>
把多个RDD中的数据根据key聚合起来
<div align=center>
    <img src="./pic/cogroup.png" width="70%", height="50%"/>
</div>

### intersection
intersection的原理：
> 1. 首先map操作变成一个tuple
> 2. 然后cogroup聚合两个RDD的key
> 3. filter, 过滤掉两个集合中任意一个集合为空的key
> 4. map，还原出单key
<div align=center>
    <img src="./pic/intersection.png" width="70%", height="50%"/>
</div>

### join
join算子的原理：
> 1. cogroup, 聚合两个rdd的key
> 2. flatMap, 聚合后每条数据，可能返回多条数据，将每个key对应两个集合做了一个笛卡儿积
<div align=center>
    <img src="./pic/join.png" width="70%", height="50%"/>
</div>

### sortByKey
sortByKey的原理：
> 1. shuffleRDD, 做shuffle read, 将相同的key拉到一个partition中来
> 2. mapPartition, 对每个partition内的key进行全局的排序
<div align=center>
    <img src="./pic/sortBykey.png" width="70%", height="50%"/>
</div>

### cartesian
cartesian的原理：</br>
<div align=center>
    <img src="./pic/cartesian.png" width="50%", height="70%"/>
</div>

### coalesce和repartition
coalesce的原理：</br>
coalesce操作使用HashPartition进行重分区，第一个参数为重分区的数目，第二个为是否进行shuffle，默认情况为false。</br>
repartition操作是coalesce函数第二个参数为true的实现
``` scala
var data = sc.textFile("...") // 假设分区数为2
var rdd1 = data.coalesce(1)   // 分区数小于原分区数目，可以正常进行
var rdd2 = data.coalesce(4)   // 如果分区数目大于原来分区数，必须指定shuffle为true，否则分区数不变
```
repartition原理如下：</br>
> 1. map, 附加了前缀，根据要重分区成几个分区，计算出前缀
> 2. shuffle -> coalesceRDD
> 3. 去掉前缀
<div align=center>
    <img src="./pic/repartition.png" width="70%", height="50%"/>
</div>

