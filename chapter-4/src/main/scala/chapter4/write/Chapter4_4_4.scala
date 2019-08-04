package chapter4.write

import org.apache.hadoop.io.compress.GzipCodec
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
object Chapter4_4_4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter4_4_4")
    val sc = new SparkContext(conf)

    val data = List(("姓名", "小明"), ("年龄", "18"))
    val rddData = sc.parallelize(data, 1)
    val path: String = "file:///G:\\BookData\\chapter4\\write\\Chapter4_4_4"
    rddData.saveAsSequenceFile(path, Some(classOf[GzipCodec]))

    sc.stop()
  }
}
