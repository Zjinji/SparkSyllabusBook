package chapter8

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Chapter8_4_1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter8_4_1")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(5))
    val lines = ssc.textFileStream("hdfs://linux01:8020/spark_streaming_test")
    lines.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
