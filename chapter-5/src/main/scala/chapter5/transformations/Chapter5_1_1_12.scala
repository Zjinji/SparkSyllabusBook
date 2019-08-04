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
object Chapter5_1_1_12 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter5_1_1_12")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(1 to 10, 3)
    val splitRDD = rddData1.randomSplit(Array(1, 4, 5))

    println(splitRDD(0).collect.mkString(","))
    println(splitRDD(1).collect.mkString(","))
    println(splitRDD(2).collect.mkString(","))

    sc.stop()
  }
}
