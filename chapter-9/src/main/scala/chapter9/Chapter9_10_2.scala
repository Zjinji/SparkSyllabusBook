package chapter9

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/**
  *
  * FUNCTIONAL_DESCRIPTION: 
  * CREATE_BY: 尽际
  * CREATE_TIME: 2019/3/5 15:46
  * MODIFICATORY_DESCRIPTION: 
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */
object Chapter9_10_2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Chapter9_10_2")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val lines = spark
      .readStream
      .format("socket")
      .option("host", "linux01")
      .option("port", 9999)
      .load()

    val words = lines.as[String]
      .flatMap(_.split(" "))
      .map(s => (s, s.reverse))
      .toDF("原单词", "单词反转")


    val query = words.writeStream
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(0))
      .format("json")
      .option("path", "G:\\BookData\\chapter9\\9_10_2")
      .option("checkpointLocation", "./checkpoint_chapter9_10_2")
      .start()

    query.awaitTermination()
  }
}
