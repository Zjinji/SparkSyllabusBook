package chapter4.write

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
object Chapter4_4_1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter4_4_1")
    val sc = new SparkContext(conf)

    val rddData = sc.parallelize(Array(("one", 1), ("two", 2), ("three", 3)), 10)
    val path = "file:///G:\\BookData\\chapter4\\write\\Chapter4_4_1"
    rddData.saveAsTextFile(path)

    sc.stop
  }
}
