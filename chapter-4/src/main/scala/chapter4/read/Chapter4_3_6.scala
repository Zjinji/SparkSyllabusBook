package chapter4.read

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.orc.mapreduce.OrcInputFormat
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
object Chapter4_3_6 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter4_3_6")

    val sc = new SparkContext(conf)

    val path = "hdfs://linux01:8020/BookData/chapter4/read/chapter4_3_6.txt"
    val inputHadoopFile = sc.newAPIHadoopFile[LongWritable, Text, TextInputFormat](path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text])

    val result = inputHadoopFile.map(_._2.toString).collect
    println(result.mkString("\n"))

    sc.stop
  }
}
