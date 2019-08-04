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
object Chapter4_4_3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter4_4_3")
    val sc = new SparkContext(conf)

    //假设某一次处理数据得到了最终结果集合
    val array = Array("Thomas", 18, "male", "65kg", "180cm")

    //转换成CSV格式保存
    val csvRDD = sc.parallelize(Array(array.mkString(",")), 1)
    val csvPath = "file:///G:\\BookData\\chapter4\\write\\Chapter4_4_3_1"
    csvRDD.saveAsTextFile(csvPath)

    //转换成TSV格式保存
    val tsvRDD = sc.parallelize(Array(array.mkString("\t")), 1)
    val tsvPath = "file:///G:\\BookData\\chapter4\\write\\Chapter4_4_3_2"
    tsvRDD.saveAsTextFile(tsvPath)

    sc.stop
  }
}
