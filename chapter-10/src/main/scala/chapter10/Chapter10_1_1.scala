package chapter10

import org.apache.spark.{SparkConf, SparkContext}

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
object Chapter10_1_1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter10_1_1")
    val sc = new SparkContext(conf)

//    优化前
//    val rddData1 = sc.parallelize(Array(("Alice", 15), ("Bob", 18), ("Thomas", 20), ("Catalina", 25)))
//    val rddData2 = sc.parallelize(Array(("Alice", "Female"), ("Thomas", "Male"), ("Tom", "Male")))
//
//    val rddData3 = rddData1.join(rddData2, 3)
//    println(rddData3.collect.mkString(","))

    //优化后
    val data1 = Map(("Alice", 15), ("Bob", 18), ("Thomas", 20), ("Catalina", 25))
    val rddData2 = sc.parallelize(Array(("Alice", "Female"), ("Thomas", "Male"), ("Tom", "Male")))

    val rddData1Broadcast = sc.broadcast(data1)
    val rddData3 = rddData2.map(t => {
      val data1Map = rddData1Broadcast.value
      if(data1Map.contains(t._1)){
        (t._1, (data1Map(t._1), t._2))
      }else{
        null
      }
    }).filter(_ != null)
    println(rddData3.collect().mkString(","))
  }
}
