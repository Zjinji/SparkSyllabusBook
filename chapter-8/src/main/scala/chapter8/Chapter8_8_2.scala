package chapter8

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf

object Chapter8_8_2 {

  val checkpointPath = "./checkpoint_Chapter8_8_2"

  def createContext(host: String, port: Int, checkpointDirectory: String): StreamingContext = {
    println("创建新的Context")
    val conf = new SparkConf().setMaster("local[*]").setAppName("Chapter8_8_2")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("WARN")
    ssc.checkpoint(checkpointDirectory)

    val lines = ssc.socketTextStream(host, port)
    val words = lines.flatMap(_.split(" "))
    val wordMap = words.map(x => (x, 1))
    wordMap.checkpoint(Seconds(5 * 5))
    val wordCounts = wordMap.reduceByKey(_ + _)
    wordCounts.print()

    ssc
  }

  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getOrCreate(
      checkpointPath,
      () => createContext("linux01", 9999, checkpointPath))
    ssc.start()
    ssc.awaitTermination()
  }
}
