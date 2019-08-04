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
object Chapter9_10_4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Chapter9_10_4")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val rdd = spark.sparkContext
      .parallelize(Seq("dog cat dog dog", "flink spark spark"))
      .flatMap(_.split(" "))

    val wordCount = rdd
      .toDF("word")
      .groupBy("word")
      .count()
      .map(row => row.getString(0) + "," + row.getLong(1).toString)
      .toDF("value")

    wordCount.write
      .format("kafka")
      .option("kafka.bootstrap.servers", "linux01:9092,linux02:9092,linux03:9092")
      .option("topic", "chapter9_10_4")
      .save()

  }
}
