package chapter8

import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Chapter8_6_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter8_6_2")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Durations.seconds(5))

    val linesDStream = ssc.socketTextStream("linux01", 9999)

    val resultDStream = linesDStream.map(_.split(",")).filter(_.length == 4)

    resultDStream.foreachRDD(rdd => {
      rdd.foreachPartition(p => {
        val conn = ConnectionPool.getConnection()
        val preparedStatement = conn.prepareStatement("INSERT INTO syllabus.t_car_position (`plate_num`, `longitude`, `latitude`, `timestamp`) VALUES (?, ?, ?, ?);")
        p.foreach(result => {
          preparedStatement.setString(1, result(0))
          preparedStatement.setFloat(2, result(1).toFloat)
          preparedStatement.setFloat(3, result(2).toFloat)
          preparedStatement.setLong(4, result(3).toLong)
          preparedStatement.addBatch()
        })
        preparedStatement.executeBatch()
        conn.commit()
        ConnectionPool.returnConnection(conn)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
