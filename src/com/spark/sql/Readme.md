# sparkSQL学习笔记

# 目录

* [sparkSQL基础操作](#sparksql基础操作)
    * [RDD, DataFrame, DataSet, SparkSession](#rdd-dataframe-dataset-sparksession)
        * [DataSet API有哪些优点](#dataset-api有哪些优点)
        * [DataSet操作](#dataset操作)
    * [RDD转为DataFrame](#rdd转为dataframe)
    * [sparkSQL udf和udaf](#sparksql-udf和udaf)
* [sparkSQL各种数据源](#sparksql各种数据源)
    * [文件](#文件)
    * [Hive](#hive)
    * [JDBC](#jdbc)
* [Thrift JDBC ODBC Server](#thrift-jdbc-odbc-server)
* [一个实战的案例](#一个实战的案例)


## sparkSQL基础操作

### RDD, DataFrame, DataSet, SparkSession
* 从spark2.0开始，Dataset有两种表现形式：type API和untyped API。我们可以认为DataFrame就是DataSet\[Row\]的别名，
Row就是一个untyped类型的对象，因为Row是类似于数据库中的一行，我们只知道有哪些列，但是有些列即使不存在，我们也可以对这些不存在的列进行操作。
因此其被定义为untyped, 就是弱类型</br>
* 而DataSet\[T\]本身，是一种typed类型的API，其中的Object通常都是我们自己自定义的，所以包括字段命名以及字段类型都是强类型的。

#### DataSet API有哪些优点</br>
1. 静态类型以及运行时的类型安全性</br>
> SQL语言具有最不严格的限制，而Dataset具有最严格的限制。SQL语言在只有在运行时才能发现一些错误，比如类型错误，但是由于Dataframe/Dataset目前都是要求类型指定的（静态类型），因此在编译时就可以发现类型错误，并提供运行时的类型安全。比如说，如果我们调用了一个不属于Dataframe的API，编译时就会报错。但是如果你使用了一个不存在的列，那么也只能到运行时才能发现了。而最严格的就是Dataset了，因为Dataset是完全基于typed API来设计的，类型都是严格而且强类型的，因此如果你使用了错误的类型，或者对不存在的列进行了操作，都能在编译时就发现。</br>


|   |  SQL  |  DataFrame  |  DataSet  |
|:--:|:----:|:-----------:|:---------:|
|Syntax Error  | Runtime | Compile Time | Compile Time |
|Analysis Error| Runtime | Runtime      | Compile Time |


2. 将半结构化的数据转换为typed自定义类型</br>

> 举例来说，如果我们现在有一份包含了学校中所有学生的信息，是以JSON字符串格式定义的，比如`{“name”: “leo”, “age”, 19, “classNo”: 1}`。我们可以自己定义一个类型，比如`case class Student(name: String, age: Integer, classNo: Integer)`。接着我们就可以加载指定的json文件，并将其转换为typed类型的`Dataset[Student]`，比如`val ds = spark.read.json("students.json").as[Student]`。

在这里，Spark会执行三个操作：
1. Spark首先会读取json文件，并且自动推断其schema，然后根据schema创建一个DataFrame。
2. 在这里，会创建一个DataFrame=DataSet\[Row\]，使用Row来存放你的数据，因为此时还不知道具体确切的类型。
3. 接着将Dataframe转换为DataSet\[Student\]，因为此时已经知道具体的类型是Student了。

这样，我们就可以将半结构化的数据，转换为自定义的typed结构化强类型数据集。并基于此，得到之前说的编译时和运行时的类型安全保障。

3. API的易用性
> Dataframe/Dataset引入了很多的high-level API，并提供了domain-specific language风格的编程接口。这样的话，大部分的计算操作，都可以通过Dataset的high-level API来完成。通过typed类型的Dataset，我们可以轻松地执行agg、select、sum、avg、map、filter、groupBy等操作。使用domain-specific language也能够轻松地实现很多计算操作，比如类似RDD算子风格的map()、filter()等。

关于DataFrame DataSet的操作案例，以及对hive的支持参见代码[SparkSQLDemo](./SparkSQLDemo.scala)

#### DataSet操作

* action操作：</br>
触发job的操作: collect, count, foreach, reduce等
[ActionOperation](./ActionOperation.scala)

* 基础操作</br>
持久化，临时视图，df与ds相互转换，写数据等[BasicOperation](./BasicOperation.scala)

* typed操作[TypedOperation](./TypedOperation.scala)
    * coalesce和repartition
    ```
    都是用来重新定义分区的
    区别在于：coalesce，只能用于减少分区数量，而且可以选择不发生shuffle
    repartiton，可以增加分区，也可以减少分区，必须会发生shuffle，相当于是进行了一次重分区操作
    ```
    * distinct和dropDuplicates
    ```
    都是用来进行去重的，区别在哪儿呢？
    distinct，是根据每一条数据，进行完整内容的比对和去重
    dropDuplicates，可以根据指定的字段进行去重
    ```
    * except filter intersect
    ```
    except：获取在当前DataSet中有，但是在另外一个DataSet中没有的元素
    filter：根据我们自己的逻辑，如果返回true，那么就保留该元素，否则就过滤掉该元素
    intersect：获取两个数据集的交集
    ```
    * map flatMap mapPartitions
    ```
    map：将数据集中的每条数据都做一个映射，返回一条新数据
    flatMap：数据集中的每条数据都可以返回多条数据
    mapPartitions：一次性对一个partition中的数据进行处理
    ```
    * joinWith
    * sort
    * randomSplit和sample
    ``` scala
    // 根据权重切分成几份
    employeeDS.randomSplit(Array(3, 10, 20))
    // 根据比例随机抽取
    employeeDS.sample(false, 0.3)
    ```

* untyped操作[DepartmentAvgSalaryAndAgeStat](./DepartmentAvgSalaryAndAgeStat.scala)  
select where groupBy agg col join </br>


* 聚合函数[AggregateFunction](./AggregateFunction.scala)
    * avg sum max min count countDistinct
    * collect_list collect_set
    
* 其它函数[OtherFunction](./OtherFunction.scala)  
   * 日期函数：current_date、current_timestamp  
   * 数学函数：round  
   * 随机函数：rand  
   * 字符串函数：concat、concat_ws
   * 聚合函数: countDistinct [DailyUV](./DailyUV.scala) 
   * 开窗函数：row_number [RowNumberWindowFunction](./RowNumberWindowFunction.scala)
   * 自定义udf和udaf函数,参见[sparkSQL udf和udaf](###sparksql-udf和udaf)</br>
   
其它函数可以参考：[spark文档](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$)



### RDD转为DataFrame

1. 使用反射的方式  

使用反射来推断包含了特定数据类型的RDD的元数据。这种基于反射的方式，代码比较简洁，当你已经知道你的RDD的元数据时，是一种非常不错的方式。
[RDD2DataFrameReflection](./RDD2DataFrameReflection.scala)

2. 使用动态加载的方式  

是通过编程接口来创建DataFrame，你可以在程序运行时动态构建一份元数据，然后将其应用到已经存在的RDD上。这种方式的代码比较冗长，但是如果在编写程序时，还不知道RDD的元数据，只有在程序运行时，才能动态得知其元数据，那么只能通过这种动态构建元数据的方式。
[RDD2DataFrameProgramatically](./RDD2DataFrameProgramatically.scala)

### sparkSQL udf和udaf

1. UDF(User Define Function)  

spark内置的函数不能满足我们的要求的时候，我们通常需要自定义函数来实现我们的需求。[UDF](./UDF.scala)

2. UDAF(User Defined Aggregate Function)  

UDAF可以针对多行输入，进行聚合计算，返回一个输出，功能更加[UDAFStringCount](./UDAFStringCount.scala)和[UDAF](./UDAF.scala)


## sparkSQL各种数据源

### 文件

**1. JSON**

Spark SQL可以自动推断JSON文件的元数据，并且加载其数据，创建一个DataFrame。可以使用SQLContext.read.json()方法，针对一个元素类型为String的RDD，或者是一个JSON文件。

但是要注意的是，这里使用的JSON文件与传统意义上的JSON文件是不一样的。每行都必须，也只能包含一个，单独的，自包含的，有效的JSON对象。不能让一个JSON对象分散在多行。否则会报错。

[JSONDataSource](./JSONDataSource.scala)

**2. Parquet**  

Parquet是面向分析型业务的列式存储格式，  
列式存储和行式存储相比有哪些优势呢？
```
1、可以跳过不符合条件的数据，只读取需要的数据，降低IO数据量。 
2、压缩编码可以降低磁盘存储空间。由于同一列的数据类型是一样的，可以使用更高效的压缩编码（例如Run Length Encoding和Delta Encoding）进一步节约存储空间。 
3、只读取需要的列，支持向量运算，能够获取更好的扫描性能。 
```

**2.1 使用编程的方式加载Parquet文件中的数据**  
和JSON一样
```
val usersDF = sqlContext.read.parquet("hdfs://spark1:9000/spark-study/users.parquet")  
usersDF.registerTempTable("users")
val userNamesDF = sqlContext.sql("select name from users")  
userNamesDF.rdd.map { row => "Name: " + row(0) }.collect()
    .foreach { userName => println(userName) }   
```

**2.2 自动分区推断**  
Spark SQL中的Parquet数据源，支持自动根据目录名推断出分区信息。例如，如果将人口数据存储在分区表中，并且使用性别和国家作为分区列。那么目录结构可能如下所示：  
```
tableName
  |- gender=male
    |- country=US
      ...
      ...
      ...
    |- country=CN
      ...
  |- gender=female
    |- country=US
      ...
    |- country=CH
      ... 
```
如果将/tableName传入SQLContext.read.parquet()或者SQLContext.read.load()方法，那么Spark SQL就会自动根据目录结构，推断出分区信息，是gender和country。即使数据文件中只包含了两列值，name和age，但是Spark SQL返回的DataFrame，调用printSchema()方法时，会打印出四个列的值：name，age，country，gender。这就是自动分区推断的功能。

此外，分区列的数据类型，也是自动被推断出来的。目前，Spark SQL仅支持自动推断出数字类型和字符串类型。有时，用户也许不希望Spark SQL自动推断分区列的数据类型。此时只要设置一个配置即可， spark.sql.sources.partitionColumnTypeInference.enabled，默认为true，即自动推断分区列的类型，设置为false，即不会自动推断类型。禁止自动推断分区列的类型时，所有分区列的类型，就统一默认都是String。

实现代码如下：
``` scala
val usersDF = sqlContext.read().parquet("hdfs://spark1:9000/spark-study/users/gender=male/country=US/users.parquet")
usersDF.printSchema()
usersDF.show()
```
**2.3 合并元数据**  
如同ProtocolBuffer，Avro，Thrift一样，Parquet也是支持元数据合并的。用户可以在一开始就定义一个简单的元数据，然后随着业务需要，逐渐往元数据中添加更多的列。在这种情况下，用户可能会创建多个Parquet文件，有着多个不同的但是却互相兼容的元数据。Parquet数据源支持自动推断出这种情况，并且进行多个Parquet文件的元数据的合并。  

因为元数据合并是一种相对耗时的操作，而且在大多数情况下不是一种必要的特性，从Spark 1.5.0版本开始，默认是关闭Parquet文件的自动合并元数据的特性的。可以通过以下两种方式开启Parquet数据源的自动合并元数据的特性：  
```
1、读取Parquet文件时，将数据源的选项，mergeSchema，设置为true  
2、使用SQLContext.setConf()方法，将spark.sql.parquet.mergeSchema参数设置为true  
```
案例代码：[ParquetMergeSchema](./ParquetMergeSchema.scala)


### Hive
Spark SQL支持对Hive中存储的数据进行读写。可以执行Hive的大部分功能，包括创建表、往表里导入数据以及用SQL语句查询表中的数据。查询出来的数据是一个Row数组。  
将hive-site.xml拷贝到spark/conf目录下，将mysql connector拷贝到spark/lib目录下
[HiveDataSource](./HiveDataSource.scala)

### JDBC
Spark SQL支持使用JDBC从关系型数据库（比如MySQL）中读取数据。读取的数据，依然由DataFrame表示，可以很方便地使用Spark Core提供的各种算子进行处理。
[JDBCDataSource](./JDBCDataSource.scala)

## Thrift JDBC ODBC Server
Spark SQL的Thrift JDBC/ODBC server是基于Hive 0.13的HiveServer2实现的。这个服务启动之后，最主要的功能就是可以让我们通过
Java JDBC来以编程的方式调用Spark SQL。此外，在启动该服务之后，可以通过Spark或Hive 0.13自带的beeline工具来进行测试。

start-thriftserver.sh命令可以接收所有spark-submit命令可以接收的参数，额外增加的一个参数是--hiveconf，可以用于指定一些
Hive的配置属性。可以通过执行./sbin/start-thriftserver.sh --help来查看所有可用参数的列表。默认情况下，启动的服务会在
localhost:10000地址上监听请求。</br>

可以使用两种方式来改变服务监听的地址
* 第一种
``` shell
export HIVE_SERVER2_THRIFT_PORT=<listening-port>
export HIVE_SERVER2_THRIFT_BIND_HOST=<listening-host>
./sbin/start-thriftserver.sh \
  --master <master-uri> \
  ...
```
* 第二种
``` shell
./sbin/start-thriftserver.sh \
  --hiveconf hive.server2.thrift.port=<listening-port> \
  --hiveconf hive.server2.thrift.bind.host=<listening-host> \
  --master <master-uri>
  ...
```
``` shell
# 启动的时候可能会有问题
hdfs dfs -chmod 777 /tmp/hive-root # 不然会报错

./sbin/start-thriftserver.sh \
--jars /usr/local/hive/lib/mysql-connector-java-5.1.17.jar
```
这两种方式的区别就在于，第一种是针对整个机器上每次启动服务都生效的; 第二种仅仅针对本次启动生效

接着就可以通过Spark或Hive的beeline工具来测试Thrift JDBC/ODBC server
在Spark的bin目录中，执行beeline命令（当然，我们也可以使用Hive自带的beeline工具）：`./bin/beeline` </br>
进入beeline命令行之后，连接到JDBC/ODBC server上去：</br>
``` shell
beeline> !connect jdbc:hive2://localhost:10000
```
如果我们想要直接通过JDBC/ODBC服务访问Spark SQL，并直接对Hive执行SQL语句，那么就需要将Hive的hive-site.xml配置文件放在Spark的conf目录下。

Thrift JDBC/ODBC server也支持通过HTTP传输协议发送thrift RPC消息。使用以下方式的配置可以启动HTTP模式：</br>

命令参数
``` shell
./sbin/start-thriftserver.sh \
  --hive.server2.transport.mode=http \
  --hive.server2.thrift.http.port=10001 \
  --hive.server2.http.endpoint=cliservice \
  --master <master-uri>
  ...
  
./sbin/start-thriftserver.sh \
  --jars /usr/local/hive/lib/mysql-connector-java-5.1.17.jar \
  --hiveconf hive.server2.transport.mode=http \
  --hiveconf hive.server2.thrift.http.port=10001 \
  --hiveconf hive.server2.http.endpoint=cliservice 
```
beeline连接服务时指定参数
``` shell
beeline> !connect jdbc:hive2://localhost:10001/default?hive.server2.transport.mode=http;hive.server2.thrift.http.path=cliservice
```
示例代码：[ThriftJDBCServerTest](./ThriftJDBCServerTest.scala)

## 一个实战的案例
就在本文件夹下有个[news](./news)文件夹, [OfflineDataGenerator](./news/OfflineDataGenerator.scala)是模拟数据的，[NewsOfflineStatSpark](./news/NewsOfflineStatSpark.scala)是主要业务实现

