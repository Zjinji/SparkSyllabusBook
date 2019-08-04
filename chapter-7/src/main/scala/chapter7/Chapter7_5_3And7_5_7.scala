package chapter7

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Chapter7_5_3And7_5_7 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Chapter7_5_2")
      .getOrCreate()

//    import spark.implicits._

    //7.5.3
    val df3_1 = spark.read.json("hdfs://linux01:8020/spark/chapter7/data/user.json")
    df3_1.show
    val df3_2 = spark.read.csv("hdfs://linux01:8020/spark/chapter7/data/user.csv").toDF("name", "age", "sex")
    df3_2.show

    //7.5.4
    val df4 = spark.read.load("hdfs://linux01:8020/spark/chapter7/data/user.snappy.parquet")

    //7.5.5_1
    val df5_1 = spark.createDataFrame(List(
      ("Alice", "Female", "20"),
      ("Tom", "Male", "25"),
      ("Boris", "Male", "18"))).toDF("name", "sex", "age")
    df5_1.show()


    //7.5.5_2
    val schema = StructType(List(
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("sex", StringType, true)
    ))

    val javaList = new java.util.ArrayList[Row]()
    javaList.add(Row("Alice", 20, "Female"))
    javaList.add(Row("Tom", 18, "Male"))
    javaList.add(Row("Boris", 30, "Male"))
    val df5_2 = spark.createDataFrame(javaList, schema)
    df5_2.show

    //7.5.6
    val options = Map("url" -> "jdbc:mysql://linux01:3306/syllabus",
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "root",
      "password" -> "123456",
      "dbtable" -> "person")
    val df6 = spark.read.format("jdbc").options(options).load()
    df6.show

    //7.5.7_1
    val df7_1 = spark.createDataFrame(List(
      ("Alice", "Female", "20"),
      ("Tom", "Male", "25"),
      ("Boris", "Male", "18"))).toDF("name", "sex", "age")

    val properties = new java.util.Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "123456")

    import org.apache.spark.sql.SaveMode
    df7_1.write.mode(SaveMode.Append).jdbc("jdbc:mysql://linux01:3306/syllabus", "t_user", properties)

    //7.5.7_2
    val df7_2 = spark.createDataFrame(List(
      ("Alice", "Female", "20"),
      ("Tom", "Male", "25"),
      ("Boris", "Male", "18"))).toDF("name", "sex", "age")
    df7_2.repartition(1).write.format("parquet").save("hdfs://linux01:8020/spark/chapter7/data/parquet")

    //7.5.7_3
    val df7_3 = spark.createDataFrame(List(
      ("Alice", "Female", "20"),
      ("Tom", "Male", "25"),
      ("Boris", "Male", "18"))).toDF("name", "sex", "age")
    df7_3.repartition(1).write.json("hdfs://linux01:8020/spark/chapter7/data/json")
    df7_3.repartition(1).write.csv("hdfs://linux01:8020/spark/chapter7/data/csv")
  }
}
