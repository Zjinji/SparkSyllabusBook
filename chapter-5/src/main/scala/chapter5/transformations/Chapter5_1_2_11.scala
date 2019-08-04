package chapter5.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * FUNCTIONAL_DESCRIPTION:
  * CREATE_BY: 尽际
  * CREATE_TIME: 2019/2/27 10:00
  * MODIFICATORY_DESCRIPTION:
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */
object Chapter5_1_2_11 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter5_1_2_11")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(
      Array(
        ("文件A", "cat\tdog\thadoop\tcat"),
        ("文件A", "cat\tdog\thadoop\tspark"),
        ("文件B", "cat\tspark\thadoop\tcat"),
        ("文件B", "spark\tdog\tspark\tc)t")),
      2)

    val rddData2 = rddData1.flatMapValues(_.split("\t"))

    val rddData3 = rddData2.map((_, 1))

    val rddData4 = rddData3.reduceByKey(_ + _).sortBy(_._1._1)

    println(rddData4.collect.mkString(","))

      sc.stop()
  }
}
