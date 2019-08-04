package chapter8

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Chapter8_2_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter8_2_2")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(5))
    val lines = ssc.socketTextStream("linux01", 9999)
    val words = lines.flatMap(_.split(" "))
    val wordMap = words.map(x => (x, 1))
    val wordCounts = wordMap.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
