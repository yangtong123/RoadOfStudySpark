# Spark 应用程序之间调度算法
在 Standalone 模式下，Master 提供里资源管理调度功能。在调度过程中，Master 先启动等待列表中应用程序的 Driver，这个 Driver 尽可能分散在集群的 Worker 节点上，
然后根据集群的内存和 CPU 使用情况，对等待运行的应用程序进行资源分配。默认分配规则是有条件的 FIFO，先分配的应用程序会尽可能多的获取满足条件的资源，
后分配的应用程序只能在剩余资源中再次筛选。如果没有合适资源的应用程序知恩感等待。`Master.scheduler`方法如下：

``` scala
private def schedule(): Unit = {
    if (state != RecoveryState.ALIVE) {
      return
    }
    // 随机打乱 Worker 节点
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

默认情况下，在 Standalone 模式下，每个应用程序可以分配到的 CPU 核数可以由`spark.deploy.defaultCores`进行设置，但是该配置默认为`Int.max`，
也就是不限制，从而应用程序会尽可能获取 CPU 资源。为了限制每个应用程序使用 CPU 资源，用户一方面可以设置`spark.core.max`配置项，约束每个应用程序所能申请的最大 CPU 核数；
另一方面可以设置`spark.executor.cores`配置项，用于设置在每个 Executor 上启动的 CPU 核数。

对于 Worker 的分配策略有两种:一种是尽量把应用程序运行可能多的 Worker 上，这种分配算法不仅能充分利用集群资源，还有利于数据本地性；另一种就是应用程序运行在尽量少的 Worker 上，
这种适用于 CPU 密集型而内存使用较少的场景。配置项为`spark.deploy.spreadOut`。主要代码为：`Master.scheduleExecutorsOnWorkers`方法实现。

``` scala
private def scheduleExecutorsOnWorkers(app: ApplicationInfo,usableWorkers: Array[WorkerInfo],spreadOutApps: Boolean): Array[Int] = {
  // 应用程序中每个 Executor 所需要的 CPU 核数
  val coresPerExecutor = app.desc.coresPerExecutor
  
  // 每个 Executor 所需的最少核数，如果设置了 coresPerExecutor 则为该值，否则为 1
  val minCoresPerExecutor = coresPerExecutor.getOrElse(1)
  
  // 如果没有设置 coresPerExecutor，那么每个 Worker 上只有一个 Executor
  val oneExecutorPerWorker = coresPerExecutor.isEmpty
  
  // 每个 Executor 需要分配多少内存
  val memoryPerExecutor = app.desc.memoryPerExecutorMB
  
  // 可用 Worker 数量
  val numUsable = usableWorkers.length
  
  // Worker 节点所能提供的 CPU 核数数组
  val assignedCores = new Array[Int](numUsable) // Number of cores to give to each worker
  
  // Worker 上 Executor 个数数组
  val assignedExecutors = new Array[Int](numUsable) // Number of new executors on each worker
  
  // 需要分配的 CPU 核数，为应用程序所需 CPU 核数和可用 CPU 核数最小值
  var coresToAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum)
  
/**
 * 返回指定的 Worker 节点是否能够启动 Executor，满足条件：
 *   1. 应用程序需要分配 CPU 核数 >= 每个 Executor 所需的最少 CPU 核数
 *   2. 是否有足够的 CPU 核数，判断条件为 (该 Worker 节点可用 CPU 核数 - 该 Worker 节点已分配的 CPU 核数) >= 每个 Executor 所需最少 CPU 核数
 * 如果在该 Worker 节点上允许启动新的 Executor，需要追加以下两个条件：
 *   1) 判断内存是否足够启动 Executor，其方法是：(当前 Worker 节点可用内存 - 该 Worker 已分配的内存) >= 每个 Executor 分配的内存大小
 *   2) 已经分配给 (该应用程序的 Executor 数量 + 已经运行该应用程序的 Executor 数量) < 该应用程序 Executor 设置的最大值
 */
def canLaunchExecutor(pos: Int): Boolean = {
  val keepScheduling = coresToAssign >= minCoresPerExecutor
  val enoughCores = usableWorkers(pos).coresFree - assignedCores(pos) >= minCoresPerExecutor

  // 启动新 Executor 条件是：该 Worker 节点允许启动多个 Executor 或者在该 Worker 节点上没有为该应用程序分配 Executor
  val launchingNewExecutor = !oneExecutorPerWorker || assignedExecutors(pos) == 0
  // 如果在该 Worker 节点上允许启动多个 Executor，那么该 Executor 节点满足启动条件就可以启动新 Executor，
  // 否则只能启动一个 Executor 并尽可能的多分配 CPU 核数
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

// 在可用的 Worker 节点中启动 Executor，在 Worker 节点每次分配资源时，分配给 Executor 所需的最少 CPU 核数，该过程是通过多次轮询进行，
// 直到没有 Worker 节点满足启动 Executor 条件活着已经达到应用程序限制。在分配过程中 Worker 节点可能多次分配，
// 如果该 Worker 节点可以启动多个 Executor，则每次分配的时候启动新的 Executor 并赋予资源；
// 如果该 Worker 节点只能启动一个 Executor，则每次分配的时候把资源追加到该 Executor
var freeWorkers = (0 until numUsable).filter(canLaunchExecutor)
while (freeWorkers.nonEmpty) {
  freeWorkers.foreach { pos =>
    var keepScheduling = true
    while (keepScheduling && canLaunchExecutor(pos)) {
      // 每次分配最少 Executor 所需 CPU 核数
      coresToAssign -= minCoresPerExecutor
      assignedCores(pos) += minCoresPerExecutor

      // 如果设置每个 Executor 启动 CPU 核数，则该 Worker 只能为该应用程序启动 1 个 Executor，
      // 否则在每次分配中启动 1 个新的 Executor
      if (oneExecutorPerWorker) {
        assignedExecutors(pos) = 1
      } else {
        assignedExecutors(pos) += 1
      }

      // 如果是分散运行，则在某一 Worker 节点上做完资源分配立即移到下一个 Worker 节点，
      // 如果是集中运行，则持续在某一 Worker 节点上做资源分配，知道使用完该 Worker 节点所有资源。
      // 传入的 Worker 节点列表是按照 CPU 核数倒序排列，在集中运行时，会尽可能少的使用 Worker 节点
      if (spreadOutApps) {
        keepScheduling = false
      }
    }
  }
  // 继续从上一次分配完的可用 Worker 节点列表获取满足启动 Executor 的 Worker 节点列表
  freeWorkers = freeWorkers.filter(canLaunchExecutor)
}
// 返回每个 Worker 节点分配的 CPU 核数
assignedCores
}
```

> tips:
   关于[Spark-8881](https://github.com/apache/spark/pull/7274)，这个算法是在 Spark 1.4.2 的版本中优化的。在以前，Worker 节点中，只能为某应用程序启动一个 Executor。轮询分配资源时，Worker 节点每次分配 1 个 CPU 核数，
   这样有可能会造成某个 Worker 节点最终分配 CPU 核数小于每个 Executor 所需 CPU 核数，那么该节点将不启动该 Executor。例如：
   在集群中有 4 个 Worker 节点，每个节点拥有 16 个 CPU 核数，其中设置了`spark.cores.max=48`和 `spark.executor.cores=16`，由于每个 Worker 只启动一个 Executor，按照每次分配一个 CPU 核数，
   则每个 Worker 节点的 Executor 将分配到 12 个 CPU 核数，每个由于 12 < 16, 所以没有 Executor 能启动。现在改进的算法是，如果设置了`spark.executor.cores`，那么每次分配的时候就分配这个指定的 CPU 核数，
   否则还是分配 1 个。
   

[返回开始](./Readme.md) 就是 Spark Core 的Readme.md

# 参考资料
[图解Spark: 核心技术与案例实战](http://www.cnblogs.com/shishanyuan/)