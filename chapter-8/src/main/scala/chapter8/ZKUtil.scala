package chapter8

import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer

object ZKUtil {
  def initZKClient(zkServers: String, sessionTimeout: Int, connectionTimeout: Int): ZkClient ={
    new ZkClient(zkServers, sessionTimeout, connectionTimeout, new ZkSerializer {
      override def serialize(data: scala.Any): Array[Byte] = {
        try {
          data.toString.getBytes("UTF-8")
        } catch {
          case _: ZkMarshallingError => null

        }
      }

      override def deserialize(bytes: Array[Byte]): AnyRef = {
        try {
          new String(bytes, "UTF-8")
        } catch {
          case _: ZkMarshallingError => null
        }
      }
    })
  }
}
