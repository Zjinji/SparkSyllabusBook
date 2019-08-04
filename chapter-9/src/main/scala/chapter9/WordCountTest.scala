package chapter9

import java.sql.Timestamp

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.SparkSession

/**
  *
  * FUNCTIONAL_DESCRIPTION: 
  * CREATE_BY: 尽际
  * CREATE_TIME: 2019/3/2 19:38
  * MODIFICATORY_DESCRIPTION: 
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */
case class WordInfoTest(uid: String, ts: Timestamp, word: String)
object WordCountTest {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.functions._

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val lines = spark.readStream
      .format("socket")
      .option("host", "linux01")
      .option("port", 9999)
      .load()

    val words = lines.as[String].map(s => {
      val arr = s.split(",")
      (arr(0), new Timestamp(arr(1).toLong), arr(2))
    }).toDF("uid", "ts", "word").as[WordInfoTest]

    val wordCounts = words
      .withWatermark("ts", "2 minutes")
//      .dropDuplicates("uid")
      .groupBy(
//        window($"ts", "10 minutes", "2 minutes"),
        $"word")
      .count()

//    val wordCounts = words
//      .withWatermark("ts", "5 seconds")
//      .dropDuplicates("uid")
//      .groupBy($"word")
//      .count()

    val query = wordCounts.writeStream
      .queryName("统计单词")
      .outputMode("update")
      .trigger(Trigger.ProcessingTime(0))
      .format("console")
      .start()

    query.awaitTermination()
  }
}
