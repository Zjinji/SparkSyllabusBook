package chapter4.write

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
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
object Chapter4_4_6 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter4_4_6")

    val sc = new SparkContext(conf)

    val rddData = sc.parallelize(List(("cat", 20), ("dog", 30), ("pig", 40), ("elephant", 10)), 1)
    val path = "hdfs://linux01:8020/BookData/chapter4/write/Chapter4_4_6"

    rddData.saveAsNewAPIHadoopFile(path, classOf[Text], classOf[IntWritable], classOf[TextOutputFormat[Text, IntWritable]])

    sc.stop
  }
}
