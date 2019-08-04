package chapter8

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Chapter8_7_3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter8_7_3")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("./checkpoint_Chapter8_7_3")
    val lines = ssc.socketTextStream("linux01", 9999)

    def updateStateFunc(iter: Iterator[(String, Seq[Int], Option[Int])]): Iterator[(String, Int)] = {
      iter.map{
        case (word, curWordCount, preWordCount) => {
          (word, curWordCount.sum + preWordCount.getOrElse(0))
        }
      }
    }
    val func = updateStateFunc _

    val words = lines.flatMap(_.split(" "))
    val wordMap = words.map(x => (x, 1))
    val resultStateDStream = wordMap.updateStateByKey[Int](
      func,
      new HashPartitioner(sc.defaultParallelism),
      true)
    resultStateDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
