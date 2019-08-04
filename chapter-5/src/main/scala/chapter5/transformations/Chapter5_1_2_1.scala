package chapter5.transformations

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
object Chapter5_1_2_1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter5_1_2_1")
    val sc = new SparkContext(conf)

    val rddData1 = sc.parallelize(Array(("Alice", 18), ("Bob", 3), ("Thomas", 20)), 2)
    import org.apache.spark.HashPartitioner
    val rddData2 = rddData1.partitionBy(new HashPartitioner(5))

    println(rddData2.partitions.length)

    sc.stop()
  }
}
