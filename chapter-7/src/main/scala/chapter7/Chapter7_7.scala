package chapter7

import org.apache.spark.sql.SparkSession


object Chapter7_7 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Chapter7_5_3")
      .getOrCreate()

    spark.sql(
      """
         CREATE TABLE IF NOT EXISTS t_fruit(
         name string,
         color string,
         count int)
         ROW FORMAT DELIMITED
         FIELDS TERMINATED BY ","
         STORED AS TEXTFILE
      """).checkpoint()

    spark.sql(
      """
        LOAD DATA LOCAL INPATH "/home/admin/modules/spark-2.3.1-bin-hadoop2.7/data/t_fruit.csv"
        INTO TABLE t_fruit
      """)

    spark.sql(
      """
         SELECT * FROM t_fruit
      """).show


  }
}
