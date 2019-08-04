package chapter7

import org.apache.spark.sql.SparkSession

object Chapter7_5_1_2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Chapter7_5_1_2")
      .getOrCreate()
    //生成DataFrame集合，读取并打印所有用户数据，统计拥有多少位用户。然后找到所有女性用户中，年龄小于25岁的用户。
    val df1 = spark.read.json("hdfs://linux01:8020/spark/chapter7/data/user.json")
    df1.createTempView("t_user")
    df1.createOrReplaceTempView("t_user")

    df1.createGlobalTempView("t_user")
    df1.createOrReplaceGlobalTempView("t_user")

    spark.newSession().sql("SELECT * FROM t_user").show
    spark.sql("SELECT * FROM t_user").show
    spark.sql("SELECT name, age, sex FROM t_user WHERE sex = 'Female' AND age < 25").show
    spark.sql("SELECT sex, MAX(age), COUNT(name) FROM t_user GROUP BY sex").show

  }
}
