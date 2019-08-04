package chapter8

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Success, Try}


object Chapter8_4_4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter8_4_4")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Durations.seconds(5))

    val kafkaInputDStream = KafkaUtils.createStream(ssc,
      "linux01:2181,linux02:2181,linux03:2181",
      "g_spark_test",
      Map("spark_streaming_test" -> 2))

    val kafkaValues = kafkaInputDStream.map(_._2)

    val kafkaSplits= kafkaValues.map(_.split(","))

    val kafkaFilters = kafkaSplits.filter(arr => {
      if(arr.length == 3){
        Try(arr(2).toInt) match {
          case Success(_) if arr(2).toInt > 3 => true
          case _ =>  false
        }
      }else{
        false
      }
    })

    val results = kafkaFilters.map(_.mkString(","))
    results.foreachRDD(rdd => {
      //在Driver端执行
      rdd.foreachPartition(p => {
        //在Worker端执行
        //如果将输出结果保存到某个数据库，可在此处实例化数据库的连接器
        p.foreach(result => {
          //在Worker端执行，保存到数据库时，在此处使用连接器。
          println(result)
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
