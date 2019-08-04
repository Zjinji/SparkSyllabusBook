package chapter8

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.{Success, Try}


object Chapter8_4_5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Chapter8_4_5")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Durations.seconds(10))

    val topics = Set("spark_streaming_test")
    val kafkaParams = mutable.Map[String, String]()
    kafkaParams.put("bootstrap.servers", "linux01:9092")
    kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaParams.put("session.timeout.ms", "30000")
    kafkaParams.put("enable.auto.commit", "false")
    kafkaParams.put("max.poll.records", "100")
    kafkaParams.put("kafka.topics", "spark_streaming_test")
    kafkaParams.put("group.id", "g_spark_test")

    val zkHost = "linux01:2181,linux02:2181,linux03:2181"
    val sessionTimeout = 120000
    val connectionTimeout = 60000
    val zkClient = ZKUtil.initZKClient(zkHost, sessionTimeout, connectionTimeout)

    val zkTopic = "spark_streaming_test"
    val zkConsumerGroupId = "g_spark_test"

    val zkTopicDir = new ZKGroupTopicDirs(zkConsumerGroupId, zkTopic)
    val zkTopicPath = zkTopicDir.consumerOffsetDir

    val childrenCount = zkClient.countChildren(zkTopicPath)
    var kafkaStream: InputDStream[(String, String)] = null
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    kafkaStream = if (childrenCount > 0) {
      val req = new TopicMetadataRequest(topics.toList, 0)
      val leaderConsumer = new SimpleConsumer("linux01", 9092, 10000, 10000, "StreamingOffsetObserver")

      val res = leaderConsumer.send(req)
      val topicMetaOption = res.topicsMetadata.headOption

      val partitions = topicMetaOption match {
        case Some(tm) =>
          tm.partitionsMetadata.map(pm => (pm.partitionId, pm.leader.get.host)).toMap[Int, String]
        case None =>
          Map[Int, String]()
      }

      for (partition <- 0 until childrenCount) {
        val partitionOffset = zkClient.readData[String](zkTopicPath + "/" + partition)
        val tp = TopicAndPartition(kafkaParams("kafka.topics"), partition)
        val requestMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)))
        val consumerMin = new SimpleConsumer(partitions(partition), 9092, 10000, 10000, "getMinOffset")
        val curOffsets = consumerMin.getOffsetsBefore(requestMin).partitionErrorAndOffsets(tp).offsets
        var nextOffset = partitionOffset.toLong
        if (curOffsets.nonEmpty && nextOffset < curOffsets.head) {
          nextOffset = curOffsets.head
        }
        fromOffsets += (tp -> nextOffset)
      }

      val messageHandler = (mam: MessageAndMetadata[String, String]) => (mam.key, mam.message)
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams.toMap, fromOffsets, messageHandler)
    } else {
      KafkaUtils.createDirectStream[
        String,
        String,
        StringDecoder,
        StringDecoder](ssc, kafkaParams.toMap, topics)
    }

    var offsetRanges: Array[OffsetRange] = null

    val kafkaInputDStream = kafkaStream.transform { rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    }

    val kafkaValues = kafkaInputDStream.map(_._2)

    val kafkaSplits = kafkaValues.map(_.split(","))

    val kafkaFilters = kafkaSplits.filter(arr => {
      if (arr.length == 3) {
        Try(arr(2).toInt) match {
          case Success(_) if arr(2).toInt > 3 => true
          case _ => false
        }
      } else {
        false
      }
    })

    val results = kafkaFilters.map(_.mkString(","))
    results.foreachRDD(rdd => {
      //在Driver端执行
      rdd.foreachPartition(p => {
        //在Worker端执行
        //如果将输出结果保存到某个数据库，可在此处实例化数据库的连接器
        p.foreach(result => {
          //在Worker端执行，保存到数据库时，在此处使用连接器。
          println(result)
        })
      })
      //ZkUtils不可序列化，所以需要在Driver端执行
      for (o <- offsetRanges) {
        ZkUtils.updatePersistentPath(zkClient, zkTopicDir.consumerOffsetDir + "/" + {
          o.partition
        }, o.fromOffset.toString)
        println("本次消息消费成功后，偏移量状态：" + o)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
