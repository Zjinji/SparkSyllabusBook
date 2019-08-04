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
object Chapter5_1_1_7 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter5_1_1_7")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(1 to 10)
    val rddData2 = sc.parallelize(1 to 20)
    val rddData3 = rddData1.union(rddData2)

    println(rddData3.collect.mkString(","))
    sc.stop()
  }
}
