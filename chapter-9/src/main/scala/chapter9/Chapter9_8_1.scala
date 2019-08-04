package chapter9

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

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
object Chapter9_8_1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Chapter9_8_1")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val javaList = new java.util.ArrayList[Row]()
    javaList.add(Row("Alice", "Female"))
    javaList.add(Row("Bob", "Male"))
    javaList.add(Row("Thomas", "Male"))

    val schema = StructType(List(
      StructField("name", StringType, nullable = false),
      StructField("sex", StringType, nullable = false)
    ))

    val staticDataFrame = spark.createDataFrame(javaList, schema)

    val lines = spark.readStream
      .format("socket")
      .option("host", "linux01")
      .option("port", 9999)
      .load()

    val streamDataFrame = lines.as[String].map(s => {
      val arr = s.split(",")
      (arr(0), arr(1).toInt)
    }).toDF("name", "age")

    val joinResult = streamDataFrame.join(staticDataFrame, "name")

    val query = joinResult.writeStream
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(0))
      .format("console")
      .start()

    query.awaitTermination()
  }
}
