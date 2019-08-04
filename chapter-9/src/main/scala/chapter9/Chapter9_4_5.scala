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
object Chapter9_4_5 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Chapter9_4_5")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val rows = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond ", 10)
      .option("rampUpTime ", 2)
      .option("numPartitions  ", 2)
      .load()

    val query = rows.writeStream
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2000))
      .format("console")
      .start()

    query.awaitTermination()
  }
}
