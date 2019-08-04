package chapter11.bean

import java.sql.Timestamp

/**
  *
  * FUNCTIONAL_DESCRIPTION:
  * studentID：学生ID
  * textbookID：教材ID
  * gradeID：年级ID
  * subjectID：科目ID
  * chapterID：章节ID
  * questionID：题目ID
  * score：题目得分，0~10分
  * answer_time：答题提交时间，yyyy-MM-dd HH:mm:ss字符串形式
  * ts: 答题提交时间，时间戳形式
  * CREATE_BY: 尽际
  * CREATE_TIME: 2019/3/25 10:13
  * MODIFICATORY_DESCRIPTION: 
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */
case class AnswerWithRecommendations(student_id: String,
                  textbook_id: String,
                  grade_id: String,
                  subject_id: String,
                  chapter_id: String,
                  question_id: String,
                  score: Int,
                  answer_time: String,
                  ts: Timestamp,
                  recommendations: String) extends Serializable
