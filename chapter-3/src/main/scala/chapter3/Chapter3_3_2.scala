package chapter3

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * FUNCTIONAL_DESCRIPTION: 
  * CREATE_BY: 尽际
  * CREATE_TIME: 2019/2/26 19:45
  * MODIFICATORY_DESCRIPTION: 
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */
object Chapter3_3_2 {
  def main(args: Array[String]): Unit = {
    //初始化SparkConf对象，设置基本任务参数
    val conf = new SparkConf()
      //设置目标Master机器地址
//      .setMaster("local[*]")
      //设置任务名称
      .setAppName("WC")
      .setMaster("spark://linux01:7077")
      .setJars(List("D:\\workspace\\idea_workspace\\SparkSyllabusBook\\chapter-3\\target\\chapter-3-1.0-SNAPSHOT.jar"))
      .setIfMissing("spark.driver.host", "192.168.216.1")


    //实例化SparkContext，Spark的对外接口，即负责用户与Spark内部的交互通信
    val sc = new SparkContext(conf)

    //读取文件并进行单词统计
    sc.textFile("hdfs://linux01:8020/words.txt")
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _, 1)
      .sortBy(_._2, false)
      .saveAsTextFile("hdfs://linux01:8020/output")

    //停止sc，结束该任务
    sc.stop()

  }
}
