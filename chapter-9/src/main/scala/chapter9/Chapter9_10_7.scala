package chapter9

import java.sql.{Connection, PreparedStatement}

import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

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
object Chapter9_10_7 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Chapter9_10_7")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val lines = spark
      .readStream
      .format("socket")
      .option("host", "linux01")
      .option("port", 9999)
      .load()


    val words = lines.as[String]
      .flatMap(_.split(" "))

    val wordCounts = words
      .groupBy("value")
      .count()
      .repartition(2)

    val query = wordCounts.writeStream
      .outputMode("update")
      .foreach(new ForeachWriter[Row] {
        var conn: Connection = _
        var preparedStatement: PreparedStatement = _
        var batchCount = 0

        override def open(partitionId: Long, epochId: Long): Boolean = {
          println("打开连接")
          println("partitionId：" + partitionId)
          println("epochId：" + epochId)
          conn = ConnectionPool.getConnection()
          val sql = "INSERT INTO syllabus.t_word_count (`word`, `count`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `word` = ?, `count` = ?;"
          preparedStatement = conn.prepareStatement(sql)
          conn != null && !conn.isClosed && conn.isValid(5 * 1000) && preparedStatement != null
        }

        override def process(row: Row): Unit = {
          println("处理数据")
          val word = row.getString(0)
          val count = row.getLong(1).toString
          println("word：" + word)
          println("count：" + count)
          preparedStatement.setString(1, word)
          preparedStatement.setString(2, count)
          preparedStatement.setString(3, word)
          preparedStatement.setString(4, count)
          preparedStatement.addBatch()
          batchCount += 1
          if(batchCount >= 100){
            preparedStatement.executeBatch()
            conn.commit()
            batchCount = 0
          }
        }

        override def close(errorOrNull: Throwable): Unit = {
          println("提交数据，释放资源")
          preparedStatement.executeBatch()
          conn.commit()
          batchCount = 0
          ConnectionPool.returnConnection(conn)
        }
      })
      .start()

    query.awaitTermination()

  }
}
