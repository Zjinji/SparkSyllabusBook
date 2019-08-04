package chapter11.producer

import java.io.{File, PrintWriter}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import chapter11.bean.Answer
import com.google.gson.Gson

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  *
  * FUNCTIONAL_DESCRIPTION: 
  * CREATE_BY: 尽际
  * CREATE_TIME: 2019/3/25 10:10
  * MODIFICATORY_DESCRIPTION: 
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */
object Simulator {
  //模拟数据
  //学生ID
  val arr1 = ArrayBuffer[String]()
  for (i <- 1 to 50) {
    arr1 += "学生ID_" + i
  }
  //教材ID
  val arr2 = Array("教材ID_1", "教材ID_2")
  //年级ID
  val arr3 = Array("年级ID_1", "年级ID_2", "年级ID_3", "年级ID_4", "年级ID_5", "年级ID_6")
  //科目ID
  val arr4 = Array("科目ID_1_数学", "科目ID_2_语文", "科目ID_3_英语")
  //章节ID
  val arr5 = Array("章节ID_chapter_1", "章节ID_chapter_2", "章节ID_chapter_3")

  //题目ID与教材、年级、科目、章节的对应关系,
  val questionMap = collection.mutable.HashMap[String, ArrayBuffer[String]]()

  var questionID = 1
  for (textbookID <- arr2; gradeID <- arr3; subjectID <- arr4; chapterID <- arr5) {
    val key = new StringBuilder()
      .append(textbookID).append("^")
      .append(gradeID).append("^")
      .append(subjectID).append("^")
      .append(chapterID)

    val questionArr = ArrayBuffer[String]()
    for (i <- 1 to 20) {
      questionArr += "题目ID_" + questionID
      questionID += 1
    }
    questionMap.put(key.toString(), questionArr)
  }

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def genQuestion() = {
    //随机教材ID
    val textbookIDRandom = arr2(Random.nextInt(arr2.length))
    //随机年级ID
    val gradeIDRandom = arr3(Random.nextInt(arr3.length))
    //随机科目ID
    val subjectIDRandom = arr4(Random.nextInt(arr4.length))
    //随机章节ID
    val chapterIDRandom = arr5(Random.nextInt(arr5.length))

    val key = new StringBuilder()
      .append(textbookIDRandom).append("^")
      .append(gradeIDRandom).append("^")
      .append(subjectIDRandom).append("^")
      .append(chapterIDRandom)

    //取出题目
    val questionArr = questionMap(key.toString())
    //随机题目ID
    val questionIDRandom = questionArr(Random.nextInt(questionArr.length))
    //随机题目扣分
    val deductScoreRandom = Random.nextInt(11)
    //随机学生ID
    val studentID = arr1(Random.nextInt(arr1.length))
    //答题时间
    val ts = System.currentTimeMillis()
    val timestamp = new Timestamp(ts)
    val answerTime = sdf.format(new Date(ts))

    Answer(studentID, textbookIDRandom, gradeIDRandom, subjectIDRandom, chapterIDRandom, questionIDRandom, deductScoreRandom, answerTime, timestamp)
  }


  //测试模拟数据
  def main(args: Array[String]): Unit = {
    val pw = new PrintWriter(new File("G:\\BookData\\chapter11\\modeling\\question_info.json"))
    val gson = new Gson()
    for (i <- 1 to 2000) {
      println(s"第{$i}条")
      val jsonString = gson.toJson(genQuestion())
      pw.write(jsonString + "\n")
      //      Thread.sleep(200)

    }
    pw.flush()
    pw.close()
  }
}
