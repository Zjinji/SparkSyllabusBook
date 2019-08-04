package chapter11.utils

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  *
  * FUNCTIONAL_DESCRIPTION: 
  * CREATE_BY: 尽际
  * CREATE_TIME: 2019/3/26 21:02
  * MODIFICATORY_DESCRIPTION: 
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */
object RedisUtil {
  //配置redis
  val host = "192.168.216.20"
  val port = 6379
  val timeout = 30000

  val config = new JedisPoolConfig
  config.setMaxTotal(200)
  config.setMaxIdle(50)
  config.setMinIdle(8) //设置最小空闲数

  config.setMaxWaitMillis(10000)
  config.setTestOnBorrow(true)
  config.setTestOnReturn(true)
  //idle 时进行连接扫描
  config.setTestWhileIdle(true)
  //表示idle object evitor两次扫描之间要sleep的毫秒数
  config.setTimeBetweenEvictionRunsMillis(30000)
  //表示idle object evitor每次扫描的最多的对象数
  config.setNumTestsPerEvictionRun(10)
  //表示一个对象至少停留在idle状态的最短时间，然后才能被idle object evitor扫描并驱逐；这一项只有在timeBetweenEvictionRunsMillis大于0时才有意义
  config.setMinEvictableIdleTimeMillis(60000)
  //连接池
  lazy val pool  = new JedisPool(config, host, port, timeout)
  //释放资源
  lazy val hook = new Thread{
    override def run() = {
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)
}
