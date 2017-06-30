# sparkSQL学习笔记

# 目录

## sparkSQL基础操作

### RDD, DataFrame, DataSet, SparkSession
从spark2.0开始，Dataset有两种表现形式：type API和untyped API。我们可以认为DataFrame就是DataSet\[Row\]的别名，
Row就是一个untyped类型的对象，因为Row是类似于数据库中的一行，我们只知道有哪些列，但是有些列即使不存在，我们也可以对这些不存在的列进行操作。
因此其被定义为untyped, 就是弱类型</br>
而DataSet\[T\]本身，是一种typed类型的API，其中的Object通常都是我们自己自定义的，所以包括字段命名以及字段类型都是强类型的。

#### DataSet API有哪些优点</br>
- 1.静态类型以及运行时的类型安全性</br>
> SQL语言具有最不严格的限制，而Dataset具有最严格的限制。SQL语言在只有在运行时才能发现一些错误，比如类型错误，但是由于Dataframe/Dataset目前都是要求类型指定的（静态类型），因此在编译时就可以发现类型错误，并提供运行时的类型安全。比如说，如果我们调用了一个不属于Dataframe的API，编译时就会报错。但是如果你使用了一个不存在的列，那么也只能到运行时才能发现了。而最严格的就是Dataset了，因为Dataset是完全基于typed API来设计的，类型都是严格而且强类型的，因此如果你使用了错误的类型，或者对不存在的列进行了操作，都能在编译时就发现。</br>

|   |  SQL  |  DataFrame  |  DataSet  |
|:--:|:----:|:-----------:|:---------:|
|Syntax Error  | Runtime | Compile Time | Compile Time |
|Analysis Error| Runtime | Runtime      | Compile Time |

### RDD转为DataFrame

### sparkSQL内置函数和开窗函数

### sparkSQL udf和udaf

### CLI

## sparkSQL各种数据源

### 文件

### Hive

### JDBC

## ThriftJDBC ODBCServer
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


## DataSet

### action操作：
