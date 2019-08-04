package chapter11.analysis.streaming

import chapter11.bean.{Answer, Rating}
import chapter11.utils.RedisUtil
import com.google.gson.Gson
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/**
  *
  * FUNCTIONAL_DESCRIPTION: 
  * CREATE_BY: 尽际
  * CREATE_TIME: 2019/3/26 10:20
  * MODIFICATORY_DESCRIPTION: 
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */
object StreamingAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("StreamingAnalysis")
      .config("spark.local.dir", "G:\\BookData\\temp")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._
    import org.apache.spark.sql.functions._

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

    //实时统计热点题（Top10）
    val result1 = spark.sql(
      """SELECT
        |  question_id, COUNT(1) AS frequency
        |FROM
        |  t_answer
        |GROUP BY
        |  question_id
        |ORDER BY
        |  frequency
        |DESC LIMIT
        |  10
      """.stripMargin)

    //实时统计答题最活跃的年级，并排序
    val result2 = spark.sql(
      """SELECT
        |  grade_id, COUNT(1) AS frequency
        |FROM
        |  t_answer
        |GROUP BY
        |  grade_id
        |ORDER BY
        |  frequency
        |DESC
      """.stripMargin)

    //实时统计每个科目的热点题（Top10）
    val result3 = spark.sql(
      """SELECT
        |  subject_id, question_id, COUNT(1) AS frequency
        |FROM
        |  t_answer
        |GROUP BY
        |  subject_id, question_id
        |ORDER BY
        |  frequency
        |DESC LIMIT
        |  10
      """.stripMargin)

    //实时统计每位学生得分最低的题目（Top10）
    val result4 = spark.sql(
      """SELECT
        |  student_id, FIRST(question_id), MIN(score)
        |FROM
        |  t_answer
        |GROUP BY
        |  student_id
      """.stripMargin)

    result1.writeStream
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(0))
      .format("console")
      .option("checkpointLocation", "./checkpoint_chapter11_1")
      .start()

    result2.writeStream
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(0))
      .format("console")
      .option("checkpointLocation", "./checkpoint_chapter11_2")
      .start()

    result3.writeStream
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(0))
      .format("console")
      .option("checkpointLocation", "./checkpoint_chapter11_3")
      .start()

    result4.writeStream
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(0))
      .format("console")
      .option("checkpointLocation", "./checkpoint_chapter11_4")
      .start()

    spark.streams.awaitAnyTermination()
  }
}
