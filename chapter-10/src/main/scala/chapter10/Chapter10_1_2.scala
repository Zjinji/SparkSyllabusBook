package chapter10

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  *
  * FUNCTIONAL_DESCRIPTION: 
  * CREATE_BY: 尽际
  * CREATE_TIME: 2019/3/20 11:33
  * MODIFICATORY_DESCRIPTION: 
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */
class Person(_name: String) extends Serializable {
  def name = _name
  override def toString: String = _name
}
object Chapter10_1_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter10_1_2")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Person]))

    val sc = new SparkContext(conf)

    val arrPerson = new ArrayBuffer[Person]()

    for(i <- 1 to 999999){
      arrPerson += new Person("姓名" + i)
    }

    val rddData1 = sc.parallelize(arrPerson, 2)
    rddData1.persist(StorageLevel.MEMORY_ONLY_SER)
    rddData1.collect()

    Thread.sleep(3600 * 1000)
  }
}
