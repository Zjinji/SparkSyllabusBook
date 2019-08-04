package chapter8

import org.apache.spark.SparkContext
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class WordCountAccumulator[T] private extends AccumulatorV2[T, mutable.Map[String, Int]] {
  private val _map: mutable.Map[String, Int] = mutable.Map()

  override def isZero: Boolean = _map.isEmpty

  override def copy(): AccumulatorV2[T, mutable.Map[String, Int]] = {
    val newAcc = new WordCountAccumulator[T]
    for(t <- _map){
      newAcc._map += t
    }
    newAcc
  }

  override def reset(): Unit = {
    _map.clear()
  }

  override def add(v: T): Unit = {
    val wordMapping = v.asInstanceOf[(String, Int)]
    _map += Tuple2(wordMapping._1, wordMapping._2 + _map.getOrElse(wordMapping._1, 0))
  }

  override def merge(other: AccumulatorV2[T, mutable.Map[String, Int]]): Unit = {
    val o = other.asInstanceOf[WordCountAccumulator[T]]
    for((x, y) <- _map){
      _map += Tuple2(x, y + o._map.getOrElse(x, 0))
      o._map.remove(x)
    }

    for(t <- o._map){
      _map += t
    }
  }

  override def value: mutable.Map[String, Int] = {
    _map
  }
}

object WordCountAccumulator{
  @volatile private var instance: WordCountAccumulator[(String, Int)] = null

  def getInstance(sc: SparkContext): WordCountAccumulator[(String, Int)] = {
    if(instance == null){
      synchronized{
        if(instance == null){
          instance = new WordCountAccumulator[(String, Int)]()
        }
      }
    }

    if(!instance.isRegistered) sc.register(instance)
    instance
  }
}
