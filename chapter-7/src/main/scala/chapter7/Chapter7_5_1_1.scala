package chapter7

import org.apache.spark.sql.SparkSession

object Chapter7_5_1_1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("实例化SparkSession示例")
      .enableHiveSupport()
      .getOrCreate()
    val sparkContext = spark.sparkContext
    val sqlContext = spark.sqlContext

    import spark.implicits._
    //生成DataFrame集合，读取并打印所有用户数据，统计拥有多少位用户。然后找到所有女性用户中，年龄小于25岁的用户。
    val df1 = spark.read.json("hdfs://linux01:8020/spark/chapter7/data/user.json")
    df1.createGlobalTempView("t_user")
    df1.show
    df1.select("name", "age", "sex").filter($"sex" === "Female").filter($"age" < 25).show
    df1.select($"name", $"age", $"sex").filter($"sex" === "Female" && $"age" < 25)
    df1.groupBy("sex").agg(Map(
      "age" -> "max",
      "name" -> "count"
    )).show()
    df1.select($"name").map(row => row.getAs[String]("name").toLowerCase).show()
  }
}
