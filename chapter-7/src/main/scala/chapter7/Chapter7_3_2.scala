package chapter7

import org.apache.spark.sql.SparkSession

object Chapter7_3_2 {
  case class User(name: String, age: BigInt, sex: String, addr: Array[String])
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Chapter7_3_2")
      .getOrCreate()

    import spark.implicits._
    //生成DataFrame集合，读取并打印所有用户数据，统计拥有多少位用户。然后找到所有女性用户中，年龄小于25岁的用户。
    val df1 = spark.read.json("hdfs://linux01:8020/spark/chapter7/data/user.json")
    df1.show
    df1.count
    df1.filter($"sex" === "Female").filter($"age" < 25).show
    //生成Dataset集合
    val ds1 = spark.read.json("hdfs://linux01:8020/spark/chapter7/data/user.json").as[User]
    ds1.show
    ds1.filter(_.sex == "Female").filter(_.age < 25).show
    spark.stop()
  }
}
