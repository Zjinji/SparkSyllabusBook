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
object Chapter5_1_2_9 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter5_1_2_9")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(
      Array(
        ("Alice", 19),
        ("Bob", 20),
        ("Thomas", 30),
        ("Catalina", 25),
        ("Kotlin", 27),
        ("Karen", 99)),
      2)

    val rddData2 = sc.parallelize(
      Array(
        ("Alice", "female"),
        ("Bob", "male"),
        ("Thomas", "male"),
        ("Catalina", "famale"),
        ("Kotlin", "female")),
      2)

    val rddData3 = sc.parallelize(
      Array(
        ("Alice", "Address1"),
        ("Alice", "Address2"),
        ("Bob", "Address3"),
        ("Thomas", "Address4"),
        ("Catalina", "Address5"),
        ("Kotlin", "Address6")),
      2)

    val rddData4 = rddData1.cogroup(rddData2, rddData3)

    println(rddData4.collect.mkString(","))
    sc.stop()
  }
}
