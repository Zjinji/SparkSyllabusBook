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
object Chapter5_2_1_6 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter5_2_1_6")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(Array(("语文", 95), ("数学", 75), ("英语", 88)), 2)
    val result = rddData1.reduce((t1, t2) => (t1._1 + "_" +t2._1, t1._2 + t2._2))
    println(result)
    
    sc.stop()
  }
}
