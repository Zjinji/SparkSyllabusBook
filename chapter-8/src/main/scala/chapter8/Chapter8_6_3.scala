package chapter8

import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Chapter8_6_3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter8_6_3")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Durations.seconds(30))

    val linesDStream1 = ssc.socketTextStream("linux01", 9999)
    val linesDStream2 = ssc.socketTextStream("linux01", 9998)

    val kvDStream1 = linesDStream1
      .map(_.split(","))
      .filter(_.length == 2)
      .map(arr => (arr(0), arr(1)))

    val kvDStream2 = linesDStream2
      .map(_.split(","))
      .filter(arr => arr.length == 2 && arr(1).toInt >= 85)
      .map(arr => (arr(0), arr(1)))

    val joinDStream = kvDStream1.join(kvDStream2)
    joinDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
