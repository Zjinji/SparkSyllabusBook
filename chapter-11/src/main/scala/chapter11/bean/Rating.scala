package chapter11.bean

/**
  *
  * FUNCTIONAL_DESCRIPTION: 
  * CREATE_BY: 尽际
  * CREATE_TIME: 2019/3/26 21:58
  * MODIFICATORY_DESCRIPTION: 
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */
case class Rating(student_id: Long, question_id: Long, rating: Float) extends Serializable
