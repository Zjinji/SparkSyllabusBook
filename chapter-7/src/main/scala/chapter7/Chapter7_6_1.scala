package chapter7

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object Chapter7_6_1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Chapter7_5_2")
      .getOrCreate()

    val schema = StructType(List(
      StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false),
      StructField("create_time", LongType, nullable = false)
    ))

    val javaList = new java.util.ArrayList[Row]()
    javaList.add(Row("Alice", 20, System.currentTimeMillis() / 1000))
    javaList.add(Row("Tom", 18, System.currentTimeMillis() / 1000))
    javaList.add(Row("Boris", 30, System.currentTimeMillis() / 1000))
    val df1 = spark.createDataFrame(javaList, schema)
    df1.show

    spark.sqlContext.dropTempTable("t_user")
    df1.createTempView("t_user")

    spark.sql("SELECT name, age, from_unixtime(create_time, 'yyyy-MM-dd HH:mm:ss') FROM t_user").show

    //使用UDF函数
    spark.udf.register("toUpperCaseUDF", (column: String) => column.toUpperCase)
    spark.sql("SELECT toUpperCaseUDF(name), age, from_unixtime(create_time, 'yyyy-MM-dd HH:mm:ss') FROM t_user").show
  }
}
