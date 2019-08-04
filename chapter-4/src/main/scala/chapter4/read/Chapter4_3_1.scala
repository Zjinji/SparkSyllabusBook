package chapter4.read

import org.apache.spark.{SparkConf, SparkContext}

/**
  * FUNCTIONAL_DESCRIPTION:
  * CREATE_BY: 尽际
  * CREATE_TIME: 2018/11/23 17:16
  * MODIFICATORY_DESCRIPTION:
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */

object Chapter4_3_1 {
  def main(args: Array[String]): Unit = {
    //初始化SparkConf对象，设置基本任务参数
    val conf = new SparkConf()
      //设置提交任务的目标Master机器地址，local为本地运行，[*]为自动分配任务线程数
      .setMaster("local[*]")
      //设置任务名称
      .setAppName("Chapter4_3_1")
    //实例化SparkContext
    val sc = new SparkContext(conf)

    //4.3.1  读取文本文件
    val inputTextFile = sc.textFile("G:\\BookData\\chapter4\\read\\chapter4_3_1.txt")
    println(inputTextFile.collect.mkString(","))

    //4.3.1  一次性读取
    val inputTextFile2 = sc.textFile("G:\\BookData\\chapter4\\read\\*.txt")
    println(inputTextFile2.collect.mkString(","))

    //停止sc，结束该任务
    sc.stop()
  }
}
