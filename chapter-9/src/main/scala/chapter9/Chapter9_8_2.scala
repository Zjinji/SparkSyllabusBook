package chapter9

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

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
object Chapter9_8_2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Chapter9_8_2")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val streamNameSex = spark.readStream
      .format("socket")
      .option("host", "linux01")
      .option("port", 9998)
      .load()
      .as[String].map(s => {
      val arr = s.split(",")
      val date = sdf.parse(arr(2))
      (arr(0), arr(1), new Timestamp(date.getTime))
    }).toDF("name1", "sex", "ts1")

    val streamNameAge = spark.readStream
      .format("socket")
      .option("host", "linux01")
      .option("port", 9999)
      .load()
      .as[String].map(s => {
      val arr = s.split(",")
      val date = sdf.parse(arr(2))
      (arr(0), arr(1).toInt, new Timestamp(date.getTime))
    }).toDF("name2", "age", "ts2")

    val streamNameSexWithWatermark = streamNameSex.withWatermark("ts1", "2 minutes")
    val streamNameAgeWithWatermark = streamNameAge.withWatermark("ts2", "1 minutes")

    val joinResult = streamNameSexWithWatermark.join(
      streamNameAgeWithWatermark,
      expr(
        """
        name1 = name2 AND
        ts2 >= ts1 AND
        ts2 <= ts1 + interval 1 minutes
        """),
      joinType = "inner")

    //    val streamNameSexWithWatermark = streamNameSex.withWatermark("ts1", "2 minutes")
    //      .select($"name1", $"sex", $"ts1", window($"ts1", "10 minutes", "2 minutes").alias("window1"))
    //
    //    val streamNameAgeWithWatermark = streamNameAge.withWatermark("ts2", "1 minutes")
    //      .select($"name2", $"age", $"ts2", window($"ts2", "10 minutes", "2 minutes").alias("window2"))
    //
    //    val joinResult = streamNameSexWithWatermark.join(streamNameAgeWithWatermark,
    //      $"name1" === $"name2" && $"window1" === $"window2",
    //      joinType = "inner")

    val query = joinResult.writeStream
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(0))
      .format("console")
      .start()

    query.awaitTermination()
  }
}
