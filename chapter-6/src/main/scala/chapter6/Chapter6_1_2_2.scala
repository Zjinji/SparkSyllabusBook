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
object Chapter6_1_2_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter6_1_2_2")
    val sc = new SparkContext(conf)

    val sogouRDD = sc.textFile("hdfs://linux01:8020/sougou/SogouQ1.txt", 8)
    val rddData1 = sogouRDD.map(t => (t.split("\t")(1), 1))
    rddData1.cache
    val rddData2 = rddData1.reduceByKey(_ + _)
    val rddData3 = rddData2.sortBy(_._2, ascending = false)

    println(rddData3.count)
    println(rddData3.take(3))
    println(rddData1.count)
    println(rddData1.take(3))

  }
}
