package chapter6

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * FUNCTIONAL_DESCRIPTION: 
  * CREATE_BY: 尽际
  * CREATE_TIME: 2019/2/27 18:32
  * MODIFICATORY_DESCRIPTION: 
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */
case class User1(name: String, telephone: String)

object Chapter6_4_3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter6_4_3")
    val sc = new SparkContext(conf)

    val userArray = Array(User1("Alice", "15837312345"),
      User1("Bob", "13937312666"),
      User1("Thomas", "13637312345"),
      User1("Tom", "18537312777"),
      User1("Boris", "13837312998"))

    val userRDD = sc.parallelize(userArray, 2)
    val userAccumulator = sc.collectionAccumulator[User1]("用户累加器")

    userRDD.foreach(user => {
      val telephone = user.telephone.reverse
      if (telephone(0) == telephone(1) && telephone(0) == telephone(2)) {
        userAccumulator.add(user)
      }
    })

    println(userAccumulator)

  }
}
