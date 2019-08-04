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
object Chapter5_1_2_4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter5_1_2_4")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(
      Array(
        ("班级1", 95f),
        ("班级2", 80f),
        ("班级1", 75f),
        ("班级3", 97f),
        ("班级2", 88f)),
      2)

    val rddData2 = rddData1.combineByKey(
      grade => (grade, 1),
      (gc: (Float, Int), grade) => (gc._1 + grade, gc._2 + 1),
      (gc1: (Float, Int), gc2: (Float, Int)) => (gc1._1 + gc2._1, gc1._2 + gc2._2))

    val rddData3 = rddData2.map(t => (t._1, t._2._1 / t._2._2))

    println(rddData3.collect.mkString(","))

    sc.stop()
  }
}
