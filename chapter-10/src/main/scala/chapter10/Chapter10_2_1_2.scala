package chapter10

import org.apache.spark.util.Utils
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import scala.util.Random

/**
  * FUNCTIONAL_DESCRIPTION:
  * CREATE_BY: 尽际
  * CREATE_TIME: 2019/2/27 10:00
  * MODIFICATORY_DESCRIPTION:
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */

class UserPartitioner(partitions: Int) extends Partitioner{
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  val random = new Random()

  def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = key match {
    case null => nonNegativeMod((random.nextFloat() * 100000).toInt.hashCode, numPartitions)
    case _ => nonNegativeMod((key.toString + "_" + (random.nextFloat() * 100000).toInt).hashCode, numPartitions)
  }

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }
}

object Chapter10_2_1_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter10_2_1_2")
    val sc = new SparkContext(conf)

    val arr = Array(
      ("用户A", "www.baidu.com"),
      ("用户A", "www.baidu.com"),
      ("用户A", "www.baidu.com"),
      ("用户A", "www.baidu.com"),
      ("用户A", "www.baidu.com"),
      ("用户A", "www.baidu.com"),
      ("用户A", "www.baidu.com"),
      ("用户A", "www.baidu.com"),
      ("用户A", "www.baidu.com"),
      ("用户A", "www.baidu.com"),
      ("用户A", "www.baidu.com"),
      ("用户A", "www.baidu.com"),
      ("用户A", "www.baidu.com"),
      ("用户A", "www.baidu.com"),
      ("用户B", "www.baidu.com"),
      ("用户B", "www.baidu.com")
    )

    val rddData = sc
      .parallelize(arr)
      .partitionBy(new UserPartitioner(2))

    val result = rddData.map(t => {
      (t._1, "http://" + t._2)
    })

    println(result.collect.mkString(","))

    Thread.sleep(3600 * 1000)
  }
}
