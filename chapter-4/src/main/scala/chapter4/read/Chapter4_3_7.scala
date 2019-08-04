package chapter4.read

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * FUNCTIONAL_DESCRIPTION:
  * CREATE_BY: 尽际
  * CREATE_TIME: 2018/11/23 17:16
  * MODIFICATORY_DESCRIPTION:
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */
object Chapter4_3_7 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter4_3_7")
    val sc = new SparkContext(conf)

    val inputMysql = new JdbcRDD(sc,
      () => {
        Class.forName("com.mysql.jdbc.Driver")
        DriverManager.getConnection("jdbc:mysql://linux01:3306/syllabus?useUnicode=true&characterEncoding=utf8", "root", "123456")
      },
      "SELECT * FROM person WHERE id >= ? and id <= ?;",
      1,
      3,
      1,
      r => (r.getInt(1), r.getString(2), r.getInt(3)))

    println("查询到的记录条目数：" + inputMysql.count)
    inputMysql.foreach(println)

    sc.stop()
  }
}
