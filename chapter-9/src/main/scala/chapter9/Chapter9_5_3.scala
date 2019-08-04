package chapter9

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/**
  *
  * FUNCTIONAL_DESCRIPTION: 
  * CREATE_BY: 尽际
  * CREATE_TIME: 2019/3/9 21:42
  * MODIFICATORY_DESCRIPTION: 
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */
object Chapter9_5_3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Chapter9_5_3")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val lines = spark.readStream
      .format("socket")
      .option("host", "linux01")
      .option("port", 9999)
      .load()

    val words = lines.as[String].map(s => {
      val arr = s.split(",")
      val date = sdf.parse(arr(0))
      (new Timestamp(date.getTime), arr(1))
    }).toDF("ts", "word")

    val wordCounts = words
      .groupBy(
              window($"ts", "10 minutes", "2 minutes"),
      $"word")
      .count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(0))
      .format("console")
      .start()

    query.awaitTermination()
  }
}
