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
object Chapter5_1_1_1 {
  def main(args: Array[String]): Unit = {
//    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.8.5")

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter5_1_1_1")
    val sc = new SparkContext(conf)

    val rddData = sc.parallelize(1 to 10)
    val rddData2 = rddData.map(_ * 10)
    println(rddData2.collect.mkString(","))

    sc.stop()
  }
}
