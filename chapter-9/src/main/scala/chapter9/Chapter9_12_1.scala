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
object Chapter9_12_1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Chapter9_12_1")
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

    val wordCounts = words.
      groupBy("value")
      .count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(0))
      .format("console")
      .queryName("aaa")
      .start()

    while (true){
      println("query.id :" + query.id)
      println("query.runId :" + query.runId)
      println("query.name :" + query.name)
      println("query.explain :" + query.explain())
      println("query.exception :" + query.exception)
      println("query.recentProgress :" + query.recentProgress.mkString(","))
      println("query.lastProgress :" + query.lastProgress)
      println("query.status :" + query.status)
      Thread.sleep(60 * 1000)
    }
  }
}
