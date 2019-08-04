package chapter9

import org.apache.spark.sql.SparkSession

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
object Chapter9_4_4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Chapter9_4_4")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._
    val inputDataFrame = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "linux01:9092,linux02:9092,linux03:9092")
      .option("subscribe", "chapter9_4_3")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()

    val keyValueDataset = inputDataFrame.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]

    val subwayDataFrame = keyValueDataset.flatMap(t => {
      val arr = t._2.split(",")
      Array((arr(0), arr(1)), (arr(0), arr(2)))
    }).toDF("city", "station_in_or_out")

    subwayDataFrame.createTempView("t_subway")

    val result = spark.sql("SELECT city, station_in_or_out, count(1) as hot FROM t_subway GROUP BY city, station_in_or_out ORDER BY city, hot desc")

    val query = result.write
      .format("console")
      .save()
  }
}
