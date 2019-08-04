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
object Chapter9_10_6 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Chapter9_10_6")
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

    val wordCounts = words
      .groupBy("value")
      .count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("memory")
      .queryName("t_memory_word_count")
      .start()

    while(true){
      Thread.sleep(3 * 1000)
      spark.sql("SELECT * FROM t_memory_word_count")
    }

    query.awaitTermination()

  }
}
