package chapter8

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Chapter8_4_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter8_4_2")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    val rddQueue = new mutable.SynchronizedQueue[RDD[Int]]
    val addQueueThread = new Thread(new Runnable {
      override def run(): Unit = {
        for(i <- 5 to 10){
          rddQueue += sc.parallelize(1 to i, 2)
          Thread.sleep(2000)
//          if(i == 6){
//            ssc.stop(false)
//          }
        }
      }
    })

    val inputDStream = ssc.queueStream(rddQueue)
    inputDStream.print()

    ssc.start()
    addQueueThread.start()
    ssc.awaitTermination()
  }
}
