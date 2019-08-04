package chapter11.producer

import java.util.concurrent.Executors


/**
  *
  * FUNCTIONAL_DESCRIPTION: 
  * CREATE_BY: 尽际
  * CREATE_TIME: 2019/3/25 12:37
  * MODIFICATORY_DESCRIPTION: 
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */

object KafkaProducer {
  val MAX_THREAD_SIZE = 2

  def main(args: Array[String]): Unit = {
    //创建线程池
    val executorService = Executors.newFixedThreadPool(MAX_THREAD_SIZE)
    //提交任务
    for(i <- 1 to MAX_THREAD_SIZE){
      executorService.submit(new ProducerThread)
    }

  }
}
