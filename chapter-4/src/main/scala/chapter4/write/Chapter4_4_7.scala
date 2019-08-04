package chapter4.write

import java.sql.DriverManager

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
object Chapter4_4_7 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter4_4_7")
    val sc = new SparkContext(conf)

    Class.forName("com.mysql.jdbc.Driver")

    val rddData = sc.parallelize(List(("老王", 30), ("Kotlin", 37)))
    rddData.foreachPartition((iter: Iterator[(String, Int)]) => {
      val conn = DriverManager.getConnection("jdbc:mysql://linux01:3306/syllabus?useUnicode=true&characterEncoding=utf8", "root", "123456")
      conn.setAutoCommit(false)
      val preparedStatement = conn.prepareStatement("INSERT INTO syllabus.person (`name`, `age`) VALUES (?, ?);")
      iter.foreach(t => {
        preparedStatement.setString(1, t._1)
        preparedStatement.setInt(2, t._2)
        preparedStatement.addBatch()
      })
      preparedStatement.executeBatch()
      conn.commit()
      conn.close()
    })
    sc.stop()
  }
}
