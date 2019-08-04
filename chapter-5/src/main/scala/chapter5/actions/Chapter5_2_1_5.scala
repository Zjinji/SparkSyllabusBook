package chapter5.actions

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
object Chapter5_2_1_5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter5_2_1_5")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(Array(("Alice", 95), ("Tom", 75), ("Thomas", 88)), 2)
    println(rddData1.takeOrdered(2)(Ordering.by(t => t._2)))

    sc.stop()
  }
}
