package chapter11.analysis.streaming

import chapter11.bean.Answer
import com.google.gson.Gson
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/**
 *
 * FUNCTIONAL_DESCRIPTION: 输出统计结果到 Kafka
 * CREATE_BY: 尽际
 * CREATE_TIME: 2019/3/26 10:20
 * MODIFICATORY_DESCRIPTION:
 * MODIFY_BY:
 * MODIFICATORY_TIME:
 * VERSION：V1.0
 */
object StreamingAnalysis_Bak {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("StreamingAnalysis")
      .setMaster("local[*]")
      .set("spark.local.dir", "F:\\BookData\\temp")
      .set("spark.default.parallelism", "3")
      .set("spark.sql.shuffle.partitions", "3")
      .set("spark.executor.instances", "3")

    val spark = SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val inputDataFrame1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "linux01:9092,linux02:9092,linux03:9092")
      .option("subscribe", "test_topic_learning_1")
      .load()

    val keyValueDataset1 = inputDataFrame1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]

    val answerDS = keyValueDataset1.map(t => {
      val gson = new Gson()
      val answer = gson.fromJson(t._2, classOf[Answer])
      answer
    })

    answerDS.createTempView("t_answer")

    //实时：统计题目被作答频次
    val result1 = spark.sql(
      """SELECT
        |  question_id, COUNT(1) AS frequency
        |FROM
        |  t_answer
        |GROUP BY
        |  question_id
      """.stripMargin).toJSON

    //实时：按照年级统计每个题目被作答的频次
    val result2 = spark.sql(
      """SELECT
        |  grade_id, COUNT(1) AS frequency
        |FROM
        |  t_answer
        |GROUP BY
        |  grade_id
      """.stripMargin).toJSON

    //实时：统计不同科目下，每个题目被作答的频次
    val result3 = spark.sql(
      """SELECT
        |  subject_id, question_id, COUNT(1) AS frequency
        |FROM
        |  t_answer
        |GROUP BY
        |  subject_id, question_id
      """.stripMargin).toJSON

    result1.writeStream
      .outputMode("update")
      .trigger(Trigger.ProcessingTime(0))
      .format("kafka")
      .option("kafka.bootstrap.servers", "linux01:9092,linux02:9092,linux03:9092")
      .option("topic", "test_topic_learning_2")
      .option("checkpointLocation", "./checkpoint_chapter11_1")
      .start()

    result2.writeStream
      .outputMode("update")
      .trigger(Trigger.ProcessingTime(0))
      .format("kafka")
      .option("kafka.bootstrap.servers", "linux01:9092,linux02:9092,linux03:9092")
      .option("topic", "test_topic_learning_3")
      .option("checkpointLocation", "./checkpoint_chapter11_2")
      .start()

    result3.writeStream
      .outputMode("update")
      .trigger(Trigger.ProcessingTime(0))
      .format("kafka")
      .option("kafka.bootstrap.servers", "linux01:9092,linux02:9092,linux03:9092")
      .option("topic", "test_topic_learning_4")
      .option("checkpointLocation", "./checkpoint_chapter11_3")
      .start()

    spark.streams.awaitAnyTermination()
  }
}
