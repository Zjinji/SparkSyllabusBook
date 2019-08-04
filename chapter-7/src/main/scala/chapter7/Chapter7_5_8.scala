package chapter7

import org.apache.spark.sql.SparkSession




object Chapter7_5_8 {
  case class User(name: String, age: Int, sex: String)
  case class Person(name: String, age: String, sex: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Chapter7_5_2")
      .getOrCreate()

    import spark.implicits._

    //RDD 转 DataFrame
    val rddData1 = spark.sparkContext.parallelize(Array(("Alice", "18", "Female"), ("Mathew", "20", "Male")))
    val df1 = rddData1.toDF("name", "age", "sex")
    df1.show
    //DataFrame 转 RDD
    df1.rdd.collect

    //RDD 转 Dataset
    val rddData2 = spark.sparkContext.parallelize(Array(("Alice", "18", "Female"), ("Mathew", "20", "Male")))
    val rddData3 = rddData2.map(t => User(t._1, t._2.toInt, t._3))
    val ds1 = rddData3.toDS()
    ds1.show
    //Dataset 转 RDD
    ds1.rdd.collect
    //DataFrame 转 Dataset
    val df2 = spark.createDataFrame(List(
      ("Alice", "Female", "20"),
      ("Tom", "Male", "25"),
      ("Boris", "Male", "18"))).toDF("name", "sex", "age")
    val ds2 = df2.as[Person]
    ds2.show
    //Dataset 转 DataFrame
    ds2.toDF().show
  }
}
