package chapter9

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.streaming.{GroupStateTimeout, Trigger}
import org.apache.spark.sql.{Row, SparkSession}

/**
  *
  * FUNCTIONAL_DESCRIPTION: 
  * CREATE_BY: 尽际
  * CREATE_TIME: 2019/3/15 10:05
  * MODIFICATORY_DESCRIPTION: 
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */
object Chapter9_9_1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Chapter9_9_1")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm")

    val wordsDataFrame = spark.readStream
      .format("socket")
      .option("host", "linux01")
      .option("port", 9999)
      .load()
      .as[String].map(s => {
      val arr = s.split(",")
      val date = sdf1.parse(arr(0))
      (new Timestamp(date.getTime), arr(1))
    }).toDF("ts", "word")

    val result = wordsDataFrame
      .withWatermark("ts", "3 minutes")
      .groupByKey[String]((row: Row) => {
      val timestamp = row.getTimestamp(0)
      val currentEventTimeMinute = sdf2.format(new Date(timestamp.getTime))
      currentEventTimeMinute + "," + row.getString(1)
    }).mapGroupsWithState[(String, Long), (String, String, Long)](GroupStateTimeout.EventTimeTimeout())((timeAndWord, iterator, groupState) => {
      println("当前数据：" + timeAndWord)
      println("当前Watermark：" + groupState.getCurrentWatermarkMs())
      println("状态是否存在：" + groupState.exists)
      println("状态是否过期：" + groupState.hasTimedOut)

      var count = 0L
      if(groupState.hasTimedOut){
        groupState.remove()
      }else if(groupState.exists){
        val groupCount = groupState.get._2
        if(groupCount >= 10){
          groupState.remove()
        }else{
          count = groupState.getOption.getOrElse((timeAndWord, 0L))._2 + iterator.size
          groupState.update(timeAndWord, count)
        }
      }else{
        count = iterator.size
        groupState.update(timeAndWord, count)
        val arr = timeAndWord.split(",")
        val timeoutTimestamp = sdf2.parse(arr(0)).getTime
        groupState.setTimeoutTimestamp(timeoutTimestamp)
      }

      if(count != 0){
        val arr = timeAndWord.split(",")
        (arr(0), arr(1), count)
      }else{
        null
      }
    }).filter(_ != null).toDF("time", "word", "count")

    val query = result.writeStream
      .outputMode("update")
      .trigger(Trigger.ProcessingTime(0))
      .format("console")
      .start()

    query.awaitTermination()

  }
}
