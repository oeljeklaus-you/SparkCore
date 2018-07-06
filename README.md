<<<<<<< HEAD
# Spark(基于1.3.1)源码分析
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;主要针对于Spark源码分析，对于比较重要的方法和代码,有注释,在熟悉Spark源码之前,首先必须了解Akka的通信,

如果不了解的可以看一下我的Demo,点击这里[SPRC](https://github.com/oeljeklaus-you/SPRC) ,这里主要进行的源码分析是:Spark集群启动的脚本、Spark作业提交
=======
# Spark核心源码解析
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;主要针对于Spark核心源码解析，对于比较重要的方法和代码,有注释,在熟悉Spark源码之前,首先必须了解Akka的通信,

如果不了解的可以看一下我的Demo,点击这里[SPRC](https://github.com/oeljeklaus-you/SPRC) ,这里主要进行的源码分析是:Spark集群启动的脚本、Spark作业提交

的脚本、Spark作业提交中SparkContext、Spark中SparkContext、Executor进程启动的流程、结合简单的WordCount程序

对于RDD执行流程进行剖析以及进行Stage划分分析和Task提交。
## 启动的脚本
>>>>>>> 8daa3625536331ebc83e170786e59d75ee843f9a

的脚本、Spark作业提交中SparkContext、Spark中SparkContext、Executor进程启动的流程、结合简单的WordCount程序

对于RDD执行流程进行剖析以及进行Stage划分分析和Task提交,对于一般的Spark的一些算子进行讲解和RDD划分,程序中重要的代码有注释，

在README中有方法的行数，可以帮助快速查找，对于不熟悉Scala的读者，提供了Scala的基本语法。
## Spark执行流程
![Spark执行流程](image/Spark执行流程.png)
执行流程描述:

1.通过Shell脚本启动Master，Master类继承Actor类，通过ActorySystem创建并启动。

2.通过Shell脚本启动Worker，Worker类继承Actor类，通过ActorySystem创建并启动。

3.Worker通过Akka或者Netty发送消息向Master注册并汇报自己的资源信息(内存以及CPU核数等)，以后就是定时汇报，保持心跳。

4.Master接受消息后保存(源码中通过持久化引擎持久化)并发送消息表示Worker注册成功，并且定时调度，移除超时的Worker。

5.通过Spark-Submit提交作业或者通过Spark Shell脚本连接集群，都会启动一个Spark进程Driver。

6.Master拿到作业后根据资源筛选Worker并与Worker通信，发送信息，主要包含Driver的地址等。

7.Worker进行收到消息后，启动Executor，Executor与Driver通信。

8.Driver端计算作业资源，transformation在Driver 端完成，划分各个Stage后提交Task给Executor。

9.Exectuor针对于每一个Task读取HDFS文件，然后计算结果，最后将计算的最终结果聚合到Driver端或者写入到持久化组件中。

## Spark脚本启动分析
具体流程如下[Spark启动脚本详解](https://github.com/oeljeklaus-you/SPRC)

## Executor启动流程

## 结合WordCount的Stage划分

## 任务提交流程

