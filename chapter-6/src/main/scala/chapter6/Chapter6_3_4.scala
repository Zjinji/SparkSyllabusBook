package chapter6

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * FUNCTIONAL_DESCRIPTION: 
  * CREATE_BY: 尽际
  * CREATE_TIME: 2019/2/27 18:32
  * MODIFICATORY_DESCRIPTION: 
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */
object Chapter6_3_4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter6_3_4")
    val sc = new SparkContext(conf)

    val rddData1 = sc.textFile("hdfs://linux01:8020/words6.3.3.txt")
    val rddData2 = rddData1.flatMap(_.split("\t"))

    val rddData3 = rddData2.map((_, 1))
    val rddData4 = rddData3.reduceByKey(_ + _, 1)

    println(rddData4.collect.mkString(","))

  }
}
