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
object Chapter5_2_3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter5_2_3")
    val sc = new SparkContext(conf)

    val numRDD = sc.parallelize(1 to 10)
    println(numRDD.sum)
    println(numRDD.max)
    println(numRDD.min)
    println(numRDD.mean)
    println(numRDD.variance)
    println(numRDD.sampleVariance)
    println(numRDD.stdev)
    println(numRDD.sampleStdev)

    sc.stop()
  }
}
