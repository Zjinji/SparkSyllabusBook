package chapter7


/**
  *
  * FUNCTIONAL_DESCRIPTION: 
  * CREATE_BY: 尽际
  * CREATE_TIME: 2018/12/25 20:52
  * MODIFICATORY_DESCRIPTION: 
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */

object Test {
  def main(args: Array[String]): Unit = {
    implicit def newList[T <: Product]: collection.mutable.ListBuffer[T] = collection.mutable.ListBuffer[T]()
    as[Dog]
    case class Dog(name: String)
  }

  def as[U : collection.mutable.ListBuffer]: Array[U] = null
}
