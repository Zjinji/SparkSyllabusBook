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
object Chapter5_1_2_3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter5_1_2_3")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(
      Array(
        ("班级1", "Alice"),
        ("班级2", "Tom"),
        ("班级1", "Catalina"),
        ("班级3", "Murdoch"),
        ("班级2", "Fisker")),
      2)

    val rddData2 = rddData1.groupByKey()

    println(rddData2.collect.mkString(","))

    sc.stop()
  }
}
