package chapter11.analysis.batch

import chapter11.bean.AnswerWithRecommendations
import org.apache.spark.sql.SparkSession

/**
  *
  * FUNCTIONAL_DESCRIPTION: 
  * CREATE_BY: 尽际
  * CREATE_TIME: 2019/3/26 18:06
  * MODIFICATORY_DESCRIPTION: 
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */
object BatchAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("BatchAnalysis")
      .config("spark.local.dir", "G:\\BookData\\temp")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val properties = new java.util.Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "123456")

//    val inputData = spark.read.json("G:\\BookData\\chapter11\\recommendations\\")
    val inputData = spark.read.jdbc(
      "jdbc:mysql://linux01:3306/learning?useUnicode=true&characterEncoding=utf8",
      "t_recommended",
      properties).as[AnswerWithRecommendations]

    inputData.createTempView("t_answer")
    inputData.show()

    //找到热点题（Top50）对应的科目，然后统计这些科目中，分别包含这几道热点题的条目数
    spark.sql(
      """SELECT
        |  subject_id, count(t_answer.question_id) AS hot_question_count
        | FROM
        |  (SELECT
        |    question_id, count(1) AS frequency
        |  FROM
        |    t_answer
        |  GROUP BY
        |    question_id
        |  ORDER BY
        |    frequency
        |  DESC LIMIT
        |    50) t1
        |JOIN
        |  t_answer
        |ON
        |  t1.question_id = t_answer.question_id
        |GROUP BY
        |  subject_id
        |ORDER BY
        |  hot_question_count
        | DESC
      """.stripMargin).show()

    //找到热点题（Top20）对应的推荐题目，然后找到推荐题目对应的科目，并统计每个科目分别包含推荐题目的条数
    spark.sql(
      """SELECT
        |    t4.subject_id,
        |    COUNT(1) AS frequency
        |FROM
        |    (SELECT
        |        DISTINCT(t3.question_id),
        |        t_answer.subject_id
        |    FROM
        |       (SELECT
        |       	   EXPLODE(SPLIT(t2.recommendations, ',')) AS question_id
        |       	FROM
        |       	    (SELECT
        |       	        *
        |       	     FROM
        |       	         (SELECT
        |                      question_id,
        |                      COUNT(1) AS frequency
        |                  FROM
        |                      t_answer
        |                  GROUP BY
        |                      question_id
        |                  ORDER BY
        |                      frequency
        |                  DESC LIMIT
        |                      20) t1
        |       	     JOIN
        |       	         t_answer
        |       	     ON
        |       	         t1.question_id = t_answer.question_id) t2) t3
        |      JOIN
        |         t_answer
        |      ON
        |         t3.question_id = t_answer.question_id) t4
        |GROUP BY
        |    t4.subject_id
        |ORDER BY
        |    frequency
        |DESC
      """.stripMargin).show


    spark.stop()
  }
}
