package chapter11.modeling

import chapter11.bean.{Answer, Rating}
import chapter11.utils.RedisUtil
import com.google.gson.Gson
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

/**
 *
 * FUNCTIONAL_DESCRIPTION:
 * CREATE_BY: 尽际
 * CREATE_TIME: 2019/3/25 16:52
 * MODIFICATORY_DESCRIPTION:
 * MODIFY_BY:
 * MODIFICATORY_TIME:
 * VERSION：V1.0
 */
object ALSModeling {

  def parseAnswerInfo(json: String): Rating = {
    val gson = new Gson()
    val answer = gson.fromJson(json, classOf[Answer])
    val studentID = answer.student_id.split("_")(1).toLong

    val questionID = answer.question_id.split("_")(1).toLong

    val rating = answer.score
    val ratingFix = if (rating <= 3) 3 else if (rating > 3 && rating <= 8) 2 else 1
    Rating(studentID, questionID, ratingFix)
  }

  def main(args: Array[String]): Unit = {
    val path = "G:\\BookData\\chapter11\\modeling\\question_info.json"
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ALSModeling")
      .config("spark.local.dir", "G:\\BookData\\temp")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val answerInfoDF = spark.sparkContext.textFile(path).map(parseAnswerInfo).toDS().cache()
    val randomSplits = answerInfoDF.randomSplit(Array(0.8, 0.2), 11L)

    val als = new ALS()
      .setRank(10)
      .setMaxIter(10)
      .setRegParam(0.09)
      .setUserCol("student_id")
      .setItemCol("question_id")
      .setRatingCol("rating")

    //训练集
    val model = als.fit(randomSplits(0).cache()).setColdStartStrategy("drop")

    //获得推荐
    val recommend = model.recommendForAllUsers(20)

    //测试集
    val predictions = model.transform(randomSplits(1).cache())

    //通过RMSE算法估算模型误差
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    //计算误差
    val rmse = evaluator.evaluate(predictions)

    //显示训练集数据
    randomSplits(0).foreach(x => println("训练集： " + x))
    //显示测试集数据
    randomSplits(1).foreach(x => println("测试集： " + x))
    //推荐结果
    recommend.foreach(x => println("学生ID：" + x(0) + " ,推荐题目 " + x(1)))
    //打印预测结果
    predictions.foreach(x => println("预测结果:  " + x))
    //输出误差
    println("模型误差评估：" + rmse)

    val jedis = RedisUtil.pool.getResource
    jedis.select(1)
    if (rmse <= 1) {
      val path = "G:\\BookData\\chapter11\\als_model\\" + System.currentTimeMillis()
      model.save(path)
      jedis.hset("als_model", "recommended_question_id", path)
    }


    answerInfoDF.unpersist()
    randomSplits(0).unpersist()
    randomSplits(1).unpersist()

    RedisUtil.pool.returnResource(jedis)
  }
}
