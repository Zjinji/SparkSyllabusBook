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
case class CityInfo(cityCode: String, cityName: String)

case class UserInfo(userID: String, telephone: String, userName: String)

object Chapter6_5_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter6_5_2")
    val sc = new SparkContext(conf)

    val cityDetailMap = Map(
      "010" -> "北京",
      "021" -> "上海",
      "020" -> "广州",
      "0755" -> "深圳")

    val userDetailMap = Map(
      "15837312345" -> ("userID_001", "Alice"),
      "15837322331" -> ("userID_002", "Bob"),
      "13637316666" -> ("userID_003", "Thomas"),
      "18537312399" -> ("userID_004", "Karen"),
      "13637312376" -> ("userID_005", "Tom"),
      "13737312908" -> ("userID_006", "Kotlin"))

    val cdmBroadcast = sc.broadcast(cityDetailMap)
    val udmBroadcast = sc.broadcast(userDetailMap)

    val userArray = Array(
      ("010", "15837322331"),
      ("010", "18537312399"),
      ("0755", "13737312908"),
      ("020", "13637312376"),
      ("020", "15837312345"))

    val userRDD = sc.parallelize(userArray, 2)

    val aggregateRDD = userRDD.aggregateByKey(collection.mutable.Set[String]())(
      (telephoneSet, telephone) => telephoneSet += telephone,
      (telephoneSet1, telephoneSet2) => telephoneSet1 ++= telephoneSet2)

    val resultRDD = aggregateRDD.map(info => {
      val cityInfo = CityInfo(info._1, cdmBroadcast.value(info._1))
      val userInfoSet = collection.mutable.Set[UserInfo]()
      for (telephone <- info._2) {
        val idAndName = udmBroadcast.value(telephone)
        val userInfo = UserInfo(idAndName._1, telephone, idAndName._2)
        userInfoSet.add(userInfo)
      }
      (cityInfo, userInfoSet)
    })

    println(resultRDD.collect.mkString(","))

    cdmBroadcast.unpersist
    udmBroadcast.unpersist
  }
}
