package chapter10

import java.sql.Connection

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.util.Random

/**
  * FUNCTIONAL_DESCRIPTION:
  * CREATE_BY: 尽际
  * CREATE_TIME: 2019/2/27 10:00
  * MODIFICATORY_DESCRIPTION:
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */

object Chapter10_2_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter10_2_2")
    val sc = new SparkContext(conf)

    val arr = Array(
      ("用户A", ("2019-04-01 20:13:33", "www.baidu.com")),
      ("用户A", ("2019-04-01 20:14:33", "www.baidu.com")),
      ("用户A", ("2019-04-01 20:15:33", "www.baidu.com")),
      ("用户A", ("2019-04-01 20:16:33", "www.baidu.com")),
      ("用户B", ("2019-04-01 20:17:33", "www.baidu.com")),
      ("用户B", ("2019-04-01 20:18:33", "www.baidu.com")),
      ("用户C", ("2019-04-01 20:19:33", "www.baidu.com")),
      ("用户D", ("2019-04-01 20:20:33", "www.baidu.com"))
    )

    val rddData = sc.parallelize(arr, 2)

    rddData.foreachPartition(iterator => {
      val conn = ConnectionPool.getConnection()
      val sql = "SELECT user_id, city FROM syllabus.t_user_base;"
      val preparedStatement = conn.prepareStatement(sql)
      val resultSet = preparedStatement.executeQuery()
      val user_id = resultSet.getString(1)
      val city = resultSet.getString(2)
      iterator.foreach(data => {

      })
    })


    Thread.sleep(3600 * 1000)
  }
}
