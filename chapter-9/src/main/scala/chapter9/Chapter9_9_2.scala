package chapter9

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, Trigger}
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
object Chapter9_9_2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Chapter9_9_2")
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
    }).toDF("ts", "gift")

    val result = wordsDataFrame
      .withWatermark("ts", "3 minutes")
      .groupByKey[String]((row: Row) => {
      val timestamp = row.getTimestamp(0)
      val currentEventTimeMinute = sdf2.format(new Date(timestamp.getTime))
      currentEventTimeMinute + "," + row.getString(1)
    }).flatMapGroupsWithState[(String, Long), (String, String, Long)](OutputMode.Update(), GroupStateTimeout.EventTimeTimeout())((giftAndTime, iterator, groupState) => {
      println("当前数据：" + giftAndTime)
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
          count = groupState.getOption.getOrElse((giftAndTime, 0L))._2 + iterator.size
          groupState.update(giftAndTime, count)
        }
      }else{
        count = iterator.size
        groupState.update(giftAndTime, count)
        val arr = giftAndTime.split(",")
        val timeoutTimestamp = sdf2.parse(arr(0)).getTime
        groupState.setTimeoutTimestamp(timeoutTimestamp)
      }
      val result = collection.mutable.ArrayBuffer[(String, String, Long)]()
      if(count != 0){
        val arr1 = giftAndTime.split(",")
        val arr2 = arr1(1).split("_")
        for(s <- arr2){
          result.append((arr1(0).trim, s.trim, count))
        }
      }
      result.iterator
    }).toDF("ts", "gift", "count")
//      .withWatermark("ts", "3 minutes")
//      .groupBy($"gift")
//      .count()

    val query = result.writeStream
      .outputMode("update")
      .trigger(Trigger.ProcessingTime(0))
      .format("console")
      .start()

    query.awaitTermination()

  }
}
