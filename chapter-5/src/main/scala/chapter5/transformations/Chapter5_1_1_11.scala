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
object Chapter5_1_1_11 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter5_1_1_11")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(1 to 100, 10)
    println(rddData1.partitions.length)

    val rddData2 = rddData1.repartition(5)
    println(rddData2.partitions.length)

    val rddData3 = rddData2.repartition(7)
    println(rddData3.partitions.length)

    sc.stop()
  }
}
