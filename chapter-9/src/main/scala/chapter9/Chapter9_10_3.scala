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
object Chapter9_10_3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Chapter9_10_3")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val lines = spark
      .readStream
      .format("socket")
      .option("host", "linux01")
      .option("port", 9999)
      .load()

    val wordCount = lines.as[String]
      .flatMap(_.split(" "))
      .groupBy("value")
      .count()
      .map(row => row.getString(0) + "," + row.getLong(1).toString)
      .toDF("value")


    val query = wordCount
      .writeStream
      .outputMode("update")
      .trigger(Trigger.Continuous(0))
      .format("kafka")
      .option("kafka.bootstrap.servers", "linux01:9092,linux02:9092,linux03:9092")
      .option("topic", "chapter9_10_3")
      .option("checkpointLocation", "./checkpoint_chapter9_10_3")
      .start()

    query.awaitTermination()
  }
}
