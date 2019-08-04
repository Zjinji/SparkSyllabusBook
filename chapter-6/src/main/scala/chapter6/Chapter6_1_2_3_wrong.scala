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
object Chapter6_1_2_3_wrong {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter6_1_2_3_wrong")
    val sc = new SparkContext(conf)

    val sogouRDD = sc.textFile("hdfs://linux01:8020/sougou/SogouQ1.txt", 8)
    sogouRDD.cache()
    val rddData1 = sogouRDD.filter(t => t.split("\t")(0).toLong >= 20111230140000L)
    val rddData2 = sogouRDD.map(t => (t.split("\t")(1), 1))
    sogouRDD.unpersist(true)

    println(rddData1.count)
    println(rddData2.count)
    println(rddData1.count)
    println(rddData2.count)

  }
}
