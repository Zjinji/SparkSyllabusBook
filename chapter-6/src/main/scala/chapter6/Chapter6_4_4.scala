package chapter6

import org.apache.spark.util.AccumulatorV2
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
case class User2(name: String, sex: String, age: Int)

class UserAccumulator[T] extends AccumulatorV2[T, Array[Int]] {
  //Array(男性, 女性, 第三性别, 12岁以下, 12~18岁)
  private val _array: Array[Int] = Array(0, 0, 0, 0, 0)

  override def isZero: Boolean = _array.mkString("").toLong == 0L

  override def copy(): AccumulatorV2[T, Array[Int]] = {
    val newAcc = new UserAccumulator[T]
    _array.copyToArray(newAcc._array)
    newAcc
  }

  override def reset(): Unit = {
    for (i <- _array.indices) {
      _array(i) = 0
    }
  }

  override def add(v: T): Unit = {
    val user = v.asInstanceOf[User2]
    if (user.sex == "Female") {
      _array(0) += 1
    } else if (user.sex == "Male") {
      _array(1) += 1
    } else {
      _array(2) += 1
    }

    if (user.age < 12) {
      _array(3) += 1
    } else if (user.age < 18) {
      _array(4) += 1
    }
  }

  override def merge(other: AccumulatorV2[T, Array[Int]]): Unit = {
    val o = other.asInstanceOf[UserAccumulator[T]]
    _array(0) += o._array(0)
    _array(1) += o._array(1)
    _array(2) += o._array(2)
    _array(3) += o._array(3)
    _array(4) += o._array(4)
  }

  override def value: Array[Int] = {
    _array
  }

  override def toString(): String = {
    getClass.getSimpleName + s"(id: $id, name: $name, value: ${value.mkString(",")})"
  }
}


object Chapter6_4_3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter6_4_3")
    val sc = new SparkContext(conf)

    val userArray = Array(User2("Alice", "Female", 11),
      User2("Bob", "Male", 16),
      User2("Thomas", "Male", 14),
      User2("Catalina", "Female", 20),
      User2("Boris", "Third", 12),
      User2("Karen", "Female", 9),
      User2("Tom", "Male", 7))

    val userRDD = sc.parallelize(userArray, 2)

    lazy val userAccumulator = new UserAccumulator[User2]
    sc.register(userAccumulator, "自定义用户累加器")

    userRDD.foreach(userAccumulator.add)

    println(userAccumulator)
  }
}
