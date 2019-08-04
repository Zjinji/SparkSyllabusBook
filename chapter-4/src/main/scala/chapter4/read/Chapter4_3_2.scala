package chapter4.read

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
  * FUNCTIONAL_DESCRIPTION:
  * CREATE_BY: 尽际
  * CREATE_TIME: 2018/11/23 17:16
  * MODIFICATORY_DESCRIPTION:
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */
object Chapter4_3_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter4_3_2")
    val sc = new SparkContext(conf)

    val inputJsonFile = sc.textFile("G:\\BookData\\chapter4\\read\\chapter4_3_2.json")

    val content = inputJsonFile.map(JSON.parseFull)
    println(content.collect.mkString("\t"))

    content.foreach(
      {
        case Some(map: Map[String, Any]) => println(map)
        case None => println("无效的Json")
        case _ => println("其他异常")
      }
    )

    sc.stop()
  }
}
