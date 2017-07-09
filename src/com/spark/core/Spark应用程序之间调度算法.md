# Spark应用程序之间调度算法
在Standalone模式下，Master提供里资源管理调度功能。在调度过程中，Master先启动等待列表中应用程序的Driver，这个Driver尽可能分散在集群的Worker节点上，
然后根据集群的内存和CPU使用情况，对等待运行的应用程序进行资源分配。默认分配规则是有条件的FIFO，先分配的应用程序会尽可能多的获取满足条件的资源，
后分配的应用程序只能在剩余资源中再次筛选。如果没有合适资源的应用程序知恩感等待。Master.scheduler方法如下：
``` scala
private def schedule(): Unit = {
    if (state != RecoveryState.ALIVE) {
      return
    }
    // 随机打乱Worker节点
    val shuffledAliveWorkers = Random.shuffle(workers.toSeq.filter(_.state == WorkerState.ALIVE))
    val numWorkersAlive = shuffledAliveWorkers.size
    var curPos = 0
    // 这是只对Standalone下的Cluster模式才生效，client模式Driver是在客户端
    for (driver <- waitingDrivers.toList) { 
      var launched = false
      var numWorkersVisited = 0
      while (numWorkersVisited < numWorkersAlive && !launched) {
        val worker = shuffledAliveWorkers(curPos)
        numWorkersVisited += 1
        if (worker.memoryFree >= driver.desc.mem && worker.coresFree >= driver.desc.cores) {
          launchDriver(worker, driver)
          waitingDrivers -= driver
          launched = true
        }
        curPos = (curPos + 1) % numWorkersAlive
      }
    }
    // 对等待应用程序按照顺序分配运行资源
    startExecutorsOnWorkers()
  }
```
默认情况下，在Standalone模式下，每个应用程序可以分配到的CPU核数可以由spark.deploy.defaultCores进行设置，但是该配置默认为Int.max，
也就是不限制，从而应用程序会尽可能获取CPU资源。为了限制每个应用程序使用CPU资源，用户一方面可以设置spark.core.max配置项，约束每个应用程序所能申请的最大CPU核数；
另一方面可以设置spark.executor.cores配置项，用于设置在每个Executor上启动的CPU核数。

对于Worker的分配策略有两种:一种是尽量把应用程序运行可能多的Worker上，这种分配算法不仅能充分利用集群资源，还有利于数据本地性；另一种就是应用程序运行在尽量少的Worker上，
这种适用于CPU密集型而内存使用较少的场景。配置项为spark.deploy.spreadOut。主要代码为：Master.scheduleExecutorsOnWorkers方法实现。
``` scala
private def scheduleExecutorsOnWorkers(app: ApplicationInfo,usableWorkers: Array[WorkerInfo],spreadOutApps: Boolean): Array[Int] = {
// 应用程序中每个Executor所需要的CPU核数
val coresPerExecutor = app.desc.coresPerExecutor
// 每个Executor所需的最少核数，如果设置了coresPerExecutor则为该值，否则为1
val minCoresPerExecutor = coresPerExecutor.getOrElse(1)
// 如果没有设置coresPerExecutor，那么每个Worker上只有一个Executor
val oneExecutorPerWorker = coresPerExecutor.isEmpty
// 每个Executor需要分配多少内存
val memoryPerExecutor = app.desc.memoryPerExecutorMB
// 可用Worker数量
val numUsable = usableWorkers.length
// Worker节点所能提供的CPU核数数组
val assignedCores = new Array[Int](numUsable) // Number of cores to give to each worker
// Worker上Executor个数数组
val assignedExecutors = new Array[Int](numUsable) // Number of new executors on each worker
// 需要分配的CPU核数，为应用程序所需CPU核数和可用CPU核数最小值
var coresToAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum)

/**
 * 返回指定的Worker节点是否能够启动Executor，满足条件：
 *   1. 应用程序需要分配CPU核数>=每个Executor所需的最少CPU核数
 *   2. 是否有足够的CPU核数，判断条件为该Worker节点可用CPU核数-该Worker节点已分配的CPU核数>=每个Executor所需最少CPU核数
 * 如果在该Worker节点上允许启动新的Executor，需要追加以下两个条件：
 *   1. 判断内存是否足够启动Executor，其方法是：当前Worker节点可用内存-该Worker已分配的内存>=每个Executor分配的内存大小
 *   2. 已经分配给该应用程序的Executor数量+已经运行该应用程序的Executor数量<该应用程序Executor设置的最大值
 */
def canLaunchExecutor(pos: Int): Boolean = {
  val keepScheduling = coresToAssign >= minCoresPerExecutor
  val enoughCores = usableWorkers(pos).coresFree - assignedCores(pos) >= minCoresPerExecutor

  // 启动新Executor条件是：该Worker节点允许启动多个Executor或者在该Worker节点上没有为该应用程序分配Executor
  val launchingNewExecutor = !oneExecutorPerWorker || assignedExecutors(pos) == 0
  // 如果在该Worker节点上允许启动多个Executor，那么该Executor节点满足启动条件就可以启动新Executor，
  // 否则只能启动一个Executor并尽可能的多分配CPU核数
  if (launchingNewExecutor) {
    val assignedMemory = assignedExecutors(pos) * memoryPerExecutor
    val enoughMemory = usableWorkers(pos).memoryFree - assignedMemory >= memoryPerExecutor
    val underLimit = assignedExecutors.sum + app.executors.size < app.executorLimit
    keepScheduling && enoughCores && enoughMemory && underLimit
  } else {
    // We're adding cores to an existing executor, so no need
    // to check memory and executor limits
    keepScheduling && enoughCores
  }
}

// 在可用的Worker节点中启动Executor，在Worker节点每次分配资源时，分配给Executor所需的最少CPU核数，该过程是通过多次轮询进行，
// 直到没有Worker节点满足启动Executor条件活着已经达到应用程序限制。在分配过程中Worker节点可能多次分配，
// 如果该Worker节点可以启动多个Executor，则每次分配的时候启动新的Executor并赋予资源；
// 如果该Worker节点只能启动一个Executor，则每次分配的时候把资源追加到该Executor
var freeWorkers = (0 until numUsable).filter(canLaunchExecutor)
while (freeWorkers.nonEmpty) {
  freeWorkers.foreach { pos =>
    var keepScheduling = true
    while (keepScheduling && canLaunchExecutor(pos)) {
      // 每次分配最少Executor所需CPU核数
      coresToAssign -= minCoresPerExecutor
      assignedCores(pos) += minCoresPerExecutor

      // 如果设置每个Executor启动CPU核数，则该Worker只能为该应用程序启动1个Executor，
      // 否则在每次分配中启动1个新的Executor
      if (oneExecutorPerWorker) {
        assignedExecutors(pos) = 1
      } else {
        assignedExecutors(pos) += 1
      }

      // 如果是分散运行，则在某一Worker节点上做完资源分配立即移到下一个Worker节点，
      // 如果是集中运行，则持续在某一Worker节点上做资源分配，知道使用完该Worker节点所有资源。
      // 传入的Worker节点列表是按照CPU核数倒序排列，在集中运行时，会尽可能少的使用Worker节点
      if (spreadOutApps) {
        keepScheduling = false
      }
    }
  }
  // 继续从上一次分配完的可用Worker节点列表获取满足启动Executor的Worker节点列表
  freeWorkers = freeWorkers.filter(canLaunchExecutor)
}
// 返回每个Worker节点分配的CPU核数
assignedCores
}
```

> tips:
   关于[Spark-8881](https://github.com/apache/spark/pull/7274)，这个算法是在Spark 1.4.2的版本中优化的。在以前，Worker节点中，只能为某应用程序启动一个Executor。轮询分配资源时，Worker节点每次分配1个CPU核数，
   这样有可能会造成某个Worker节点最终分配CPU核数小于每个Executor所需CPU核数，那么该节点将不启动该Executor。例如：
   在集群中有4个Worker节点，每个节点拥有16个CPU核数，其中设置了spark.cores.max=48和spark.executor.cores=16，由于每个Worker只启动一个Executor，按照每次分配一个CPU核数，
   则每个Worker节点的Executor将分配到12个CPU核数，每个由于12<16, 所以没有Executor能启动。现在改进的算法是，如果设置了spark.executor.cores，那么每次分配的时候就分配这个指定的CPU核数，
   否则还是分配1个。
   

[返回目录](./Readme.md)

# 参考资料
[图解Spark: 核心技术与案例实战](http://www.cnblogs.com/shishanyuan/)