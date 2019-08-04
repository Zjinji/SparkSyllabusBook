package chapter10

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  *
  * FUNCTIONAL_DESCRIPTION: 
  * CREATE_BY: 尽际
  * CREATE_TIME: 2019/3/20 11:33
  * MODIFICATORY_DESCRIPTION: 
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */
object Chapter10_1_3{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Chapter10_1_3")
      .set("spark.executor.memory", "6g")
      .set("spark.driver.memory", "6g")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Person]))
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(1 to 10)

    rdd.foreach(i => {
      //单条处理数据
    })

    rdd.foreachPartition(iterator => {
      for(i <- iterator){
        //批量处理数据
      }
    })

  }
}
