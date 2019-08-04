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
object Chapter6_4_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter6_4_2")
    val sc = new SparkContext(conf)

    val visitorRDD = sc.parallelize(
      Array(
        ("Bob", 15),
        ("Thomas", 28),
        ("Tom", 18),
        ("Galen", 35),
        ("Catalina", 12),
        ("Karen", 9),
        ("Boris", 20)),
      3)

    val visitorAccumulator = sc.longAccumulator("统计成年游客人数")

    visitorRDD.foreach(visitor => {
      if (visitor._2 >= 18) {
        visitorAccumulator.add(1)
      }
    })

    println(visitorAccumulator)
  }
}
