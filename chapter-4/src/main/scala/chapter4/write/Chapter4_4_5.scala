package chapter4.write

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
object Chapter4_4_5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter4_4_5")

    val sc = new SparkContext(conf)

    val person1 = Person("小明", 20)
    val person2 = Person("Alice", 18)

    val rddData = sc.parallelize(List(person1, person2), 1)
    val path = "file:///G:\\BookData\\chapter4\\write\\Chapter4_4_5"
    rddData.saveAsObjectFile(path)

    sc.stop()

  }
}


