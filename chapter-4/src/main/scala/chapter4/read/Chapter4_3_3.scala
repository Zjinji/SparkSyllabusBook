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
object Chapter4_3_3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter4_3_3")
    val sc = new SparkContext(conf)

    //读取CSV文件
    val inputCSVFile = sc.textFile("G:\\BookData\\chapter4\\read\\chapter4_3_3_1.csv").flatMap(_.split(",")).collect
    inputCSVFile.foreach(println)

    //读取TSV文件
    val inputTSVFile = sc.textFile("G:\\BookData\\chapter4\\read\\chapter4_3_3_2.tsv").flatMap(_.split("\t")).collect
    inputTSVFile.foreach(println)

    //停止sc，结束该任务
    sc.stop()
  }
}
