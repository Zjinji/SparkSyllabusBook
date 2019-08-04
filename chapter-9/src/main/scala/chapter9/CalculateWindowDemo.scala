package chapter9

import java.text.SimpleDateFormat
import java.util.Date

/**
  * 恢复自：TimeWindowing
  * FUNCTIONAL_DESCRIPTION: 
  * CREATE_BY: 尽际
  * CREATE_TIME: 2019/3/8 18:36
  * MODIFICATORY_DESCRIPTION: 
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */
object CalculateWindowDemo {
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
    //事件时间
    val eventTime = sdf.parse("2019-03-08 11:53:00")
    val eventTimestamp = eventTime.getTime
    println(eventTimestamp)
    //起始时间
    val startTime = 0L
    //窗口时间宽度：10 minutes
    val windowDuration = 10 * 60 * 1000L
    //窗口滑动长度：2 minutes
    val slideDuration = 2 * 60 * 1000L

    if(slideDuration > windowDuration) {
      println("slideDuration必须小于或等于windowDuration")
      return
    }

    //根据窗口宽度与窗口滑动步长，计算生成的window个数，即窗口连续滑动多少次，滑动长度的总和才等于窗口宽度。如果滑动次数为小数，则进位。
    val overlappingWindows = math.ceil(windowDuration * 1.0 / slideDuration).toInt
    //生成每一个window的起始时间和结束时间
    for(i <- 0 until overlappingWindows){
      //根据事件时间，按照slideDuration长度，计算供需多少次滚动，或者说是需要连续生成多少个window，才可以包裹当前事件
      val division = (eventTimestamp - startTime) / slideDuration.toDouble
      //如果无法整除，则意味着需要一个新的窗口容纳事件，即，小数部分直接进位。
      val ceil = math.ceil(division)
      //如果恰巧整除，即，移动次数恰巧为整数，则需要新增一个窗口，这是因为window的起始时间和结束时间的区间为“前闭后开”，即[startTime, endTime)
      //如果没有被整除，则已经进位，无需再+1。最后将该值作为windowId。
      val windowId = if(ceil == division) ceil + 1 else ceil
      //计算窗口起始时间 = ()
      val windowStart = (windowId + i - overlappingWindows) * slideDuration + startTime
      //计算窗口的结束时间 = 当前窗口的起始时间 + 滑动长度
      val windowEnd = windowStart + windowDuration

      if(eventTimestamp >= windowStart.toLong && eventTimestamp < windowEnd.toLong){
        val startTimeString = sdf.format(new Date(windowStart.toLong))
        val endTimeString = sdf.format(new Date(windowEnd.toLong))
        println("[" + startTimeString + ", " + endTimeString + ")")
      }
    }
  }
}
