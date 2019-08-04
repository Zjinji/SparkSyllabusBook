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
object Chapter5_1_1_13 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter5_1_1_13")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(1 to 10, 5)
    val rddData2 = rddData1.glom

    rddData2.collect().foreach(arr => println(arr.mkString(",")))
    sc.stop()
  }
}
