package chapter4.read

import chapter4.Person
import org.apache.spark.{SparkConf, SparkContext}

/**
  * FUNCTIONAL_DESCRIPTION:
  * CREATE_BY: 尽际
  * CREATE_TIME: 2018/11/23 17:16
  * MODIFICATORY_DESCRIPTION:
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */
object Chapter4_3_5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter4_3_5")

    val sc = new SparkContext(conf)

    val path = "file:///G:\\BookData\\chapter4\\read\\chapter4_3_5.object"
    val rddData = sc.objectFile[Person](path)
    println(rddData.collect.toList)

    sc.stop()
  }
}
