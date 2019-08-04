package chapter9

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

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
object Chapter9_4_1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Chapter9_4_1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val userSchema = StructType(List(
      StructField("name", StringType, nullable = false),
      StructField("sex", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false)
    ))

    val user = spark
      .readStream
      .format("csv")
      .schema(userSchema)
      .load("G:\\BookData\\chapter9\\9_4_1")

    user.createTempView("t_user")

    val result = spark.sql("SELECT sex, AVG(age) FROM t_user GROUP BY sex")

    val query = result.writeStream
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(0))
      .format("console")
      .start()

    query.awaitTermination()
  }
}
