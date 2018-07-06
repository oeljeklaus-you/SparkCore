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