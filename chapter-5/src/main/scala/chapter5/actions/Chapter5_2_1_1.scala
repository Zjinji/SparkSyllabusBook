package chapter5.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * FUNCTIONAL_DESCRIPTION:
  * CREATE_BY: 尽际
  * CREATE_TIME: 2019/2/27 10:00
  * MODIFICATORY_DESCRIPTION:
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */
object Chapter5_2_1_1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter5_2_1_1")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(1 to 5)
    println(rddData1.collect.mkString(","))

    sc.stop()
  }
}
