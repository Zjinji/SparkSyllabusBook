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
object Chapter9_14 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Chapter9_14")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    val inputDataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "linux01:9092,linux02:9092,linux03:9092")
      .option("subscribe", "chapter9_14")
      .load()

    val keyValueDataset = inputDataFrame
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val wordDataFrame = keyValueDataset
      .flatMap(_._2.split(" "))
      .toDF("word")

    val query = wordDataFrame.writeStream
      .outputMode("update")
      .option("checkpointLocation", "./checkpoint_chapter9_14")
      .trigger(Trigger.Continuous(1 * 60 * 1000L))
      .format("console")
      .start()

    query.awaitTermination()
  }
}
