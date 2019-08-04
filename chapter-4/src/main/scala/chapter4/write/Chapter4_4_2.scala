package chapter4.write

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.{JSONArray, JSONObject}

/**
  * FUNCTIONAL_DESCRIPTION:
  * CREATE_BY: 尽际
  * CREATE_TIME: 2018/11/23 17:16
  * MODIFICATORY_DESCRIPTION:
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */
object Chapter4_4_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter4_4_2")
    val sc = new SparkContext(conf)

    val map1 = Map("name" -> "Thomas", "age" -> "20", "address" -> JSONArray(List("通信地址1", "通信地址2")))
    val map2 = Map("name" -> "Alice", "age" -> "18", "address" -> JSONArray(List("通信地址1", "通信地址2", "通信地址3")))

    val rddData = sc.parallelize(List(JSONObject(map1), JSONObject(map2)), 1)
    val path = "file:///G:\\BookData\\chapter4\\write\\Chapter4_4_2"
    rddData.saveAsTextFile(path)

    sc.stop()
  }
}
