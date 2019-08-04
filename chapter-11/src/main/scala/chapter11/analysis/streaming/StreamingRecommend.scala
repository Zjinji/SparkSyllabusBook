package chapter11.analysis.streaming

import chapter11.bean.{Answer, AnswerWithRecommendations, Rating}
import chapter11.utils.RedisUtil
import com.google.gson.Gson
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

/**
  *
  * FUNCTIONAL_DESCRIPTION: 
  * CREATE_BY: 尽际
  * CREATE_TIME: 2019/3/26 22:15
  * MODIFICATORY_DESCRIPTION: 
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */
object StreamingRecommend {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("StreamingRecommend")
      .config("spark.local.dir", "G:\\BookData\\temp")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    val kafkaInputDStream = KafkaUtils.createStream(ssc,
      "linux01:2181,linux02:2181,linux03:2181",
      "g_spark_test",
      Map("test_topic_learning_1" -> 2))

    val kafkaValues = kafkaInputDStream.map(_._2)
    import spark.implicits._

    val answerDF = kafkaValues.map(json => {
      val gson = new Gson()
      val answer = gson.fromJson(json, classOf[Answer])
      answer
    })

    answerDF.foreachRDD(rdd => {
      val properties = new java.util.Properties()
      properties.setProperty("user", "root")
      properties.setProperty("password", "123456")

      val answerDF = rdd.toDS()
      val answerInfo = answerDF.map(answer => {
        val studentID = answer.student_id.split("_")(1).toLong
        Rating(studentID, 0, 0)
      })

      val jedis = RedisUtil.pool.getResource
      jedis.select(1)
      //加载模型
      val modelPath = jedis.hget("als_model", "recommended_question_id")
      val model = ALSModel.load(modelPath)
      val recommendDF = model.recommendForUserSubset(answerInfo, 20)

      val transRecommendDF = recommendDF.map(row => {
        val buffer = ArrayBuffer[String]()
        val id = row.getInt(0)
        val arr = row.getSeq[GenericRowWithSchema](1)

        for(i <- arr){
          val recommendation_question_id = "题目ID_" + i.getInt(0)
          buffer += recommendation_question_id
        }
        val student_id = "学生ID_" + id
        (student_id, buffer.toArray[String].mkString(","))
      }).toDF("student_id", "recommendations")

      val result = answerDF.join(transRecommendDF, "student_id").as[AnswerWithRecommendations].coalesce(1)
      result.show()
      val count = result.count()

      if(count > 0){
        //可以输出到HDFS、HIVE、HBASE、ES、Redis等外部存储。
        import org.apache.spark.sql.SaveMode
        result.write
          .mode(SaveMode.Append)
          .jdbc(
            "jdbc:mysql://linux01:3306/learning?useUnicode=true&characterEncoding=utf8",
            "t_recommended",
            properties)
      }

      RedisUtil.pool.returnResource(jedis)
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
