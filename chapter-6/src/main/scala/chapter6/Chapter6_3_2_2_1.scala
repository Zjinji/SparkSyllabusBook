package chapter6

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * FUNCTIONAL_DESCRIPTION: 
  * CREATE_BY: 尽际
  * CREATE_TIME: 2019/2/27 18:32
  * MODIFICATORY_DESCRIPTION: 
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */
object Chapter6_3_2_2_1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter6_3_2_2_1")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(Array(("Alice", 15), ("Bob", 18), ("Thomas", 20), ("Catalina", 25)))
    val rddData2 = sc.parallelize(Array(("Alice", "Female"), ("Thomas", "Male"), ("Tom", "Male")))
    val rddData3 = rddData1.join(rddData2)
    println(rddData3.collect.mkString(","))

  }
}
