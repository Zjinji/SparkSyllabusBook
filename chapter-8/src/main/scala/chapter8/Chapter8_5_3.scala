package chapter8

import com.alibaba.fastjson.{JSON, TypeReference}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object Chapter8_5_3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter8_5_3")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))

    //{"city":"北京","temperature":"12.5"}
    val jsonDStream = ssc.socketTextStream("linux01", 9999)
    val cityAndTemperatureDStream = jsonDStream.map(json => {
      val json2JavaMap = JSON.parseObject(json, new TypeReference[java.util.Map[String, String]](){})
      import scala.collection.JavaConverters._
      val json2ScalaMap = json2JavaMap.asScala
      json2ScalaMap
    })

    val sumOfTemperatureAndCountDStream = cityAndTemperatureDStream
        .map(scalaMap => (scalaMap("city"), scalaMap("temperature")))
        .mapValues(temperature => (temperature.toFloat, 1))
      .window()
        .reduceByKeyAndWindow(
          (t1: (Float, Int), t2: (Float, Int)) => (t1._1 + t2._1, t1._2 + t2._2),
          Seconds(10),
          Seconds(2)
        )

    val resultDStream = sumOfTemperatureAndCountDStream.mapValues(x => x._1 / x._2)
    resultDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
