package chapter6

import org.apache.spark.storage.StorageLevel
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
object Chapter6_2_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter6_2_2")
    val sc = new SparkContext(conf)

    sc.setCheckpointDir("hdfs://linux01:8020/spark_checkpoint/chapter6")

    val rddData1 = sc.parallelize(1 to 10, 2)
    val rddData2 = rddData1.map(_ * 2)
    rddData2.dependencies.head.rdd

    rddData2.persist(StorageLevel.DISK_ONLY)
    rddData2.checkpoint

    rddData2.dependencies.head.rdd
    val rddData3 = rddData2.map(_ + 3)
    val rddData4 = rddData2.map(_ + 4)

    rddData3.collect()
    rddData4.collect()

    rddData2.dependencies.head.rdd
    rddData2.unpersist(true)
  }
}
