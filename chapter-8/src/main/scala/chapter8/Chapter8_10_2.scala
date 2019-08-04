package chapter8

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


object Chapter8_10_2 {
  var hadoopConf: Configuration = _
  val shutdownMarkerPath = new Path("hdfs://linux01:8020/user/admin/tmp/spark_shutdown_marker")
  var stopMarker: Boolean = false

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter8_10_2")
//      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("WARN")

    hadoopConf = ssc.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.defaultFS", "hdfs://linux01:8020")

    val lines = ssc.socketTextStream("linux01", 9999)
    val words = lines.flatMap(_.split(" "))
    val wordMap = words.map(x => (x, 1))
    val wordCounts = wordMap.reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    val checkIntervalMillis = 2 * 1000 * 1
    var isStopped = false

    while (!isStopped) {
      println("正在确认关闭状态: ")
      isStopped = ssc.awaitTerminationOrTimeout(checkIntervalMillis)
      if (isStopped)
        println("Spark Streaming Chapter8_10已关闭.")
      else
        println("Spark Streaming Chapter8_10运行中...")
      checkShutdownMarker
      if (!isStopped && stopMarker) {
        println("准备关闭Spark Streaming")
        ssc.stop(true, true)
      }
    }
  }

  def checkShutdownMarker = {
    if (!stopMarker) {
      val fs = FileSystem.get(hadoopConf)
      stopMarker = fs.exists(shutdownMarkerPath)
    }
  }
}
