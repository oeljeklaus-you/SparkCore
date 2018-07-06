# WordCount的Stage划分
## WordCount的代码
<pre><code>
package cn.edu.hust;
object WordCount
{
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("WordCount")
    //创建SparkContext对象
    val sc=new SparkContext(conf)
    //TODO WordCount的主要流程,saveAsTextFile这个Action才开始提交任务
    sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile(args(1))
    //释放资源
    sc.stop()

  }
}
</code></pre> 
主要是从HDFS读取文件后进行单词切割,然后进行计数,如果不懂RDD算子可以看[RDD详解](https://github.com/oeljeklaus-you/SPRC)
## WordCount的各个算子
![WordCount的RDD依赖](image/WordCount的RDD依赖.png)
## SparkRDD的运行流程
![RDD运行流程](image/RDD运行流程.png)
## SparkRDD宽依赖和窄依赖
![RDD运行流程](image/SparkRDD的依赖关系.png)

SparkRDD之间的依赖主要有:

**1.宽依赖**

宽依赖指的是多个子RDD的Partition会依赖同一个父RDD的Partition

总结：窄依赖我们形象的比喻为超生


**2.窄依赖**

窄依赖指的是每一个父RDD的Partition最多被子RDD的一个Partition使用

总结：窄依赖我们形象的比喻为独生子女

## 结合WordCount的源码分析

### WordCount算子内部解析
在WordCount程序中,第一个使用的Spark方法是textFile()方法,主要的源码是
<pre><code>
 // TODO 创建一个HadoopRDD
  def textFile(path: String, minPartitions: Int = defaultMinPartitions): RDD[String] = {
    assertNotStopped()
    hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      minPartitions).map(pair => pair._2.toString).setName(path)
  }
</code></pre> 
这个方法的主要作用是从HDFS中读取数据, 这里创建一个HadoopRDD,在这个方法内部还创建一个MapPartitionRDD,接下里的几个
RDD同样是MapPartitionRDD,最主要的是看saveAsTextFile()方法。
下面是saveAsTextFile()方法,代码在RDD类的1272行,具体内容如下:
<pre><code>
//TODO 保存结果
  def saveAsTextFile(path: String) {
    // https://issues.apache.org/jira/browse/SPARK-2075
    //
    // NullWritable is a `Comparable` in Hadoop 1.+, so the compiler cannot find an implicit
    // Ordering for it and will use the default `null`. However, it's a `Comparable[NullWritable]`
    // in Hadoop 2.+, so the compiler will call the implicit `Ordering.ordered` method to create an
    // Ordering for `NullWritable`. That's why the compiler will generate different anonymous
    // classes for `saveAsTextFile` in Hadoop 1.+ and Hadoop 2.+.
    //
    // Therefore, here we provide an explicit Ordering `null` to make sure the compiler generate
    // same bytecodes for `saveAsTextFile`.
    val nullWritableClassTag = implicitly[ClassTag[NullWritable]]
    val textClassTag = implicitly[ClassTag[Text]]
    //TODO 最后写入到HDFS中还会产生一个RDD,MapPartitionsRDD
    val r = this.mapPartitions { iter =>
      val text = new Text()
      iter.map { x =>
        text.set(x.toString)
        (NullWritable.get(), text)
      }
    }
    //TODO saveAsHadoopFile
    RDD.rddToPairRDDFunctions(r)(nullWritableClassTag, textClassTag, null)
      .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path)
  }
</code></pre>
这个方法的主要作用是**产生一个RDD,MapPartitionsRDD;然后将RDD转化为PairRDDFuctions**,接下来是saveAsHadoopFile()方法:
主要的代码如下:
<pre><code>
 //TODO 准备一下Hadoop参数
    for (c <- codec) {
      hadoopConf.setCompressMapOutput(true)
      hadoopConf.set("mapred.output.compress", "true")
      hadoopConf.setMapOutputCompressorClass(c)
      hadoopConf.set("mapred.output.compression.codec", c.getCanonicalName)
      hadoopConf.set("mapred.output.compression.type", CompressionType.BLOCK.toString)
    }

    // Use configured output committer if already set
    if (conf.getOutputCommitter == null) {
      hadoopConf.setOutputCommitter(classOf[FileOutputCommitter])
    }

    FileOutputFormat.setOutputPath(hadoopConf,
      SparkHadoopWriter.createPathFromString(path, hadoopConf))
    //TODO saveAsHadoopDataset
    saveAsHadoopDataset(hadoopConf)
</code></pre>
继续查看saveAsHadoopDataset()方法源码,主要的代码如下:
<pre><code>
//TODO 拿到写入HDFS中的文件流
    val writer = new SparkHadoopWriter(hadoopConf)
    writer.preSetup()

    //TODO 一个函数将分区数据迭代的写入到HDFS中
    val writeToFile = (context: TaskContext, iter: Iterator[(K, V)]) => {
      val config = wrappedConf.value
      // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
      // around by taking a mod. We expect that no task will be attempted 2 billion times.
      val taskAttemptId = (context.taskAttemptId % Int.MaxValue).toInt

      val (outputMetrics, bytesWrittenCallback) = initHadoopOutputMetrics(context)

      writer.setup(context.stageId, context.partitionId, taskAttemptId)
      writer.open()
      var recordsWritten = 0L
      try {
        while (iter.hasNext) {
          val record = iter.next()
          writer.write(record._1.asInstanceOf[AnyRef], record._2.asInstanceOf[AnyRef])

          // Update bytes written metric every few records
          maybeUpdateOutputMetrics(bytesWrittenCallback, outputMetrics, recordsWritten)
          recordsWritten += 1
        }
      } finally {
        writer.close()
      }
      writer.commit()
      bytesWrittenCallback.foreach { fn => outputMetrics.setBytesWritten(fn()) }
      outputMetrics.setRecordsWritten(recordsWritten)
    }

    //TODO 开始提交作业,Self表示Final RDD也就是作业最后的RDD在WordCount中也就是MapPartitionsRDD
    self.context.runJob(self, writeToFile)
    writer.commitJob()
</code></pre>
代码解析:
1.获取写入HDFS中的文件流
2.一个函数将分区数据迭代的写入到HDFS中
3.开始提交作业,Self表示Final RDD也就是作业最后的RDD在WordCount中也就是MapPartitionsRDD
这里我们将会追踪到runJob()方法中,
<pre><code>
//TODO 将最后一个RDD和一个函数(writeToFile)传入到该方法中
  def runJob[T, U: ClassTag](rdd: RDD[T], func: (TaskContext, Iterator[T]) => U): Array[U] = {
    //TODO 将分区数量取出来
    runJob(rdd, func, 0 until rdd.partitions.size, false)
  }
</code></pre>
这里我们继续追踪到runJob()的重载方法,夏满是这个方法的核心代码:
<pre><code>
//TODO 重要:这个方法开始进行Stage划分
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      allowLocal: Boolean,
      resultHandler: (Int, U) => Unit) {
    if (stopped) {
      throw new IllegalStateException("SparkContext has been shutdown")
    }
    val callSite = getCallSite
    val cleanedFunc = clean(func)
    logInfo("Starting job: " + callSite.shortForm)
    if (conf.getBoolean("spark.logLineage", false)) {
      logInfo("RDD's recursive dependencies:\n" + rdd.toDebugString)
    }
    //TODO 将Stage划分后转化为TaskSet提交给TaskScheduler在交个Executor执行
    dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, allowLocal,
      resultHandler, localProperties.get)
    progressBar.foreach(_.finishAll())
    rdd.doCheckpoint()
  }
</code></pre>
这里是非常重要的方法,主要做的工作是调用SparkContext类中创建的dagScheduler,使用dagScheduler划分Stage,然后将Stage转化为TaskSet交给TaskScheduler在交个Executor执行

### 划分Stage
在前面的分析中,我们已经知道了dagScheduler调用了runJob()方法,这个方法的作用是划分stage。
<pre><code>
 //TODO 用于切分stage
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      allowLocal: Boolean,
      resultHandler: (Int, U) => Unit,
      properties: Properties): Unit = {
    val start = System.nanoTime
    //TODO 调用submitJob()返回一个调度器
    val waiter = submitJob(rdd, func, partitions, callSite, allowLocal, resultHandler, properties)
    waiter.awaitResult() match {
      case JobSucceeded => {
        logInfo("Job %d finished: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
      }
      case JobFailed(exception: Exception) =>
        logInfo("Job %d failed: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
        throw exception
    }
  }
 </code></pre>
 这里主要是划分stage,然后调用submitJob()返回一个调度器,这里我们继续查看submitJob()方法。
<pre><code>
 //TODO eventProcessLoop对象内部有一个阻塞队列和线程，先将数据封装到Case Class中将事件放入到阻塞队列中
     eventProcessLoop.post(JobSubmitted(
       jobId, rdd, func2, partitions.toArray, allowLocal, callSite, waiter, properties))
     waiter
</code></pre>
 上面是submitJob()方法的核心代码,主要的作用是eventProcessLoop对象内部有一个阻塞队列和线程，先将数据封装到Case Class中将事件放入到阻塞队列。
 
 对于JobSubmitted类的模式匹配,主要的代码如下:
<pre><code>
  //TODO 通过模式匹配判断事件的类型
     case JobSubmitted(jobId, rdd, func, partitions, allowLocal, callSite, listener, properties) =>
       //TODO 调用dagScheduler的handleJobSubmitted()方法进行处理
       dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, allowLocal, callSite,
         listener, properties)
</code></pre>
这里调用dagScheduler的**handleJobSubmitted()方法,这个方法是对stage划分的主要方法**,主要的核心代码:
<pre><code>
 //TODO 特别重要:该方法用于划分Stage 这里可以看出分区的数量决定Task数量
      finalStage = newStage(finalRDD, partitions.size, None, jobId, callSite)
  //TODO 开始提交Stage
      submitStage(finalStage)
</code></pre>
**通过newStage()方法,根据这个方法在这里可以看出分区的数量决定Task数量**。
通过追踪newStage()方法,主要的代码如下:
<pre><code>
//TODO 特别重要:主要用于划分Stage
  private def newStage(
      rdd: RDD[_],
      numTasks: Int,
      shuffleDep: Option[ShuffleDependency[_, _, _]],
      jobId: Int,
      callSite: CallSite)
    : Stage =
  {
    //TODO 得到父Stage,进行递归操作进行划分Stage
    val parentStages = getParentStages(rdd, jobId)
    val id = nextStageId.getAndIncrement()
    val stage = new Stage(id, rdd, numTasks, shuffleDep, parentStages, jobId, callSite)
    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId, stage)
    //TODO 这个Stage是最后的Stage
    stage
  }
</code></pre>
这个方法是递归的划分Stage,主要的方法是getParentStages(rdd, jobId),具体的划分代码如下:
<pre><code>
//TODO 获取和创建父stage
  private def getParentStages(rdd: RDD[_], jobId: Int): List[Stage] = {
    //TODO 这里用于保存Stage
    val parents = new HashSet[Stage]
    //TODO 判断RDD是否在这个数据结构中
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    //TODO 用于递归的查找
    val waitingForVisit = new Stack[RDD[_]]
    //TODO 从后向前进行Stage划分
    def visit(r: RDD[_]) {
      if (!visited(r)) {
        visited += r
        // Kind of ugly: need to register RDDs with the cache here since
        // we can't do it in its constructor because # of partitions is unknown
        //TODO 这里使用循环，一个RDD可能有多个依赖
        for (dep <- r.dependencies) {
          dep match {
              //TODO 进行类型匹配，如果是ShuffleDependency就可以划分成为一个新的Stage
            case shufDep: ShuffleDependency[_, _, _] =>
              //TODO 将Stage传入到该方法中去获得父Stage
              parents += getShuffleMapStage(shufDep, jobId)
              //TODO 如果是一般的窄依赖,那么将会入栈
            case _ =>
              waitingForVisit.push(dep.rdd)
          }
        }
      }
    }
    waitingForVisit.push(rdd)
    //TODO 使用栈来进行递归操作
    while (!waitingForVisit.isEmpty) {

      visit(waitingForVisit.pop())
    }
    parents.toList
  }
</code></pre>
stage划分算法如下:
涉及的数据结构:栈、HashSet
1.通过最后的RDD,获取父RDD
2.将finalRDD放入栈中,然后出栈,进行for循环的找到RDD的依赖,需要注意的是RDD可能有多个依赖
3.如果RDD依赖是ShuffleDependency,那么就可以划分成为一个新的Stage,然后通过getShuffleMapStage()获取这个stage的父stage;如果是一般的窄依赖,那么将会入栈
4.通过getShuffleMapStage()递归调用,得到父stage;一直到父stage是null
5.最后返回stage的集合

### stage提交算法
在对于最后一个RDD划stage后,进行提交stage,主要的方法是:
<pre><code>
 //TODO 递归提交stage，先将第一个stage提交
  private def submitStage(stage: Stage) {
    val jobId = activeJobForStage(stage)
    if (jobId.isDefined) {
      logDebug("submitStage(" + stage + ")")
      //TODO 判断stage是否有父Stage
      if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
        //TODO 获取没有提交的stage
        val missing = getMissingParentStages(stage).sortBy(_.id)
        logDebug("missing: " + missing)
        //TODO 这里是递归调用的终止条件，也就是第一个Stage开始提交
        if (missing == Nil) {
          logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
          submitMissingTasks(stage, jobId.get)
        } else {
          //TODO 其实就是从前向后提交stage
          for (parent <- missing) {
            submitStage(parent)
          }
          waitingStages += stage
        }
      }
    } else {
      abortStage(stage, "No active job for stage " + stage.id)
    }
  }
</code></pre>
这里和划分stage的算法一样,拿到最后的stage然后找到第一个stage开始从第一个stage开始提交。
### stage提交
下面的代码是submitMissingTasks(),主要是核心的代码:
<pre><code>
//TODO 创建多少个Task，Task数量由分区数量决定
    val tasks: Seq[Task[_]] = if (stage.isShuffleMap) {
      partitionsToCompute.map { id =>
        val locs = getPreferredLocs(stage.rdd, id)
        val part = stage.rdd.partitions(id)
        //TODO 这里进行分区局部聚合,从上游拉去数据
        new ShuffleMapTask(stage.id, taskBinary, part, locs)
      }
    } else {
      val job = stage.resultOfJob.get
      partitionsToCompute.map { id =>
        val p: Int = job.partitions(id)
        val part = stage.rdd.partitions(p)
        val locs = getPreferredLocs(stage.rdd, p)
        //TODO 将结果写入持久化介质.比如HDFS等
        new ResultTask(stage.id, taskBinary, part, locs, id)
      }
      
       //TODO 调用taskScheduler来进行提交Task，这里使用TaskSet进行封装Task
      taskScheduler.submitTasks(
              new TaskSet(tasks.toArray, stage.id, stage.newAttemptId(), stage.jobId, properties))
</code></pre>
这里主要做的工作是根据分区数量决定Task数量,然后根据stage的类型创建Task,这里主要有ShuffleMapTask和ResultTask。

ShuffleMapTask:进行分区局部聚合,从上游拉去数据。

ResultTask:将结果写入持久化介质.比如HDFS等。

这里将Task进行封装成为TaskSet进行提交给taskScheduler。

## 关于Stage划分流程图
![Stage划分流程](image/Stage划分流程.png)
## 总结
1.textFile()方法会产生两个RDD,HadoopRDD和MapPartitionRDD
2.saveTextAsFile()方法会产生一个RDD,MapPartitionRDD
3.Task数量取决于HDFS分区数量
4.Stage划分是通过最后的RDD,也就是final RDD根据依赖关系进行递归划分
5.stage提交主要是通过递归算法,根据最后一个Stage划分然后递归找到第一个stage开始从第一个stage开始提交。