package chapter8

import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Success, Try}


object Chapter8_6_1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter8_6_1")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Durations.seconds(5))

    val linesDStream = ssc.socketTextStream("linux01", 9999)

    val resultDStream = linesDStream.transform(rdd => {
      val arrayRDD = rdd.map(_.split(","))
      val filterRDD = arrayRDD.filter(arr => {
        if(arr.length == 3){
          Try(arr(2).toInt) match {
            case Success(_) if arr(2).toInt > 3 => true
            case _ =>  false
          }
        }else{
          false
        }
      })
      val resultRDD = filterRDD.map(_.mkString(","))
      resultRDD
    })

    resultDStream.foreachRDD(rdd => {
      rdd.foreachPartition(p => {
        p.foreach(result => {
          println(result)
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
