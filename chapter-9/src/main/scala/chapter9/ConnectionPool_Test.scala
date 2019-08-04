package chapter9

import java.sql.{Connection, DriverManager}
import java.util.concurrent.ConcurrentLinkedQueue

object ConnectionPool_Test {
  @volatile private var queue: ConcurrentLinkedQueue[Connection] = _

  Class.forName("com.mysql.jdbc.Driver")

  def getConnection() = {
    if(queue == null) {
      this.synchronized{
        if(queue == null){
          queue = new ConcurrentLinkedQueue[Connection]()
          for (i <- 1 to 10) {
            val conn = DriverManager.getConnection(
              "jdbc:mysql://linux01:3306/syllabus?useUnicode=true&characterEncoding=utf8",
              "root",
              "123456")
            conn.setAutoCommit(false)
            queue.offer(conn)
          }
        }
      }
    }
    queue.poll()
  }

  def returnConnection(conn: Connection) = {
    queue.offer(conn)
  }
}
