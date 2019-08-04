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
object Chapter5_2_1_7 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter5_2_1_7")
    val sc = new SparkContext(conf)

    import collection.mutable.ListBuffer
    val rddData1 = sc.parallelize(
      Array(
        ("用户1", "接口1"),
        ("用户2", "接口1"),
        ("用户1", "接口1"),
        ("用户1", "接口2"),
        ("用户2", "接口3")),
      2)

    val result = rddData1.aggregate(ListBuffer[(String)]())(
      (list: ListBuffer[String], tuple: (String, String)) => list += tuple._2,
      (list1: ListBuffer[String], list2: ListBuffer[String]) => list1 ++= list2)

    println(result)
    sc.stop()
  }
}
