package org.inh3rit.spark.kafka

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.{BrokerNotAvailableException, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import kafka.utils.{Json, ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}

import scala.collection.Seq
import scala.reflect.ClassTag

class KafkaManager() extends Serializable {

  def createDirectStream[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag]
  (ssc: StreamingContext, kafkaParams: Map[String, String], topics: Set[String]): InputDStream[(K, V)] = {
    val fromOffsets = getFromOffsets(kafkaParams, topics) //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
    val kafkaStream: InputDStream[(K, V)] = KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](
      ssc, kafkaParams, fromOffsets, (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message))
    kafkaStream
  }

  def updateZkOffset(rdd: RDD[(String, String)], topics: Set[String], kafkaParams: Map[String, String]): Unit = {
    val offsetsList: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    val kc = new KafkaCluster(kafkaParams)
    for (offsets <- offsetsList) {
      //TopicAndPartition 主构造参数第一个是topic，第二个是 partition id
      topics.foreach(topic => {
        val topicAndPartition = TopicAndPartition(topic, offsets.partition) //offsets.partition表示的是Kafka partition id
        //实现offset写入zookeeper
        val o = kc.setConsumerOffsets(kafkaParams("group_id"), Map((topicAndPartition, offsets.untilOffset)))
        if (o.isLeft)
          println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
      })
    }
  }

  private def getMaxOffset(tp: TopicAndPartition, zkClient: ZkClient): Long = {
    //从最新的消息开始消费 也就是End的位置
    val request = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
    ZkUtils.getLeaderForPartition(zkClient, tp.topic, tp.partition) match {
      case Some(brokerId) => {
        ZkUtils.readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath + "/" + brokerId)._1 match {
          case Some(brokerInfoString) => {
            Json.parseFull(brokerInfoString) match {
              case Some(m) =>
                val brokerInfo = m.asInstanceOf[Map[String, Any]]
                val host = brokerInfo.get("host").get.asInstanceOf[String]
                val port = brokerInfo.get("port").get.asInstanceOf[Int]
                new SimpleConsumer(host, port, 10000, 100000, "getMaxOffset")
                  .getOffsetsBefore(request)
                  .partitionErrorAndOffsets(tp)
                  .offsets
                  .head
              case None =>
                throw new BrokerNotAvailableException("Broker id %d does not exist".format(brokerId))
            }
          }
          case None =>
            throw new BrokerNotAvailableException("Broker id %d does not exist".format(brokerId))
        }
      }
    }
  }

  private def getMinOffset(tp: TopicAndPartition, zkClient: ZkClient): Long = {
    //从最初的消息开始消费  获取最初的消息位置  也就是 Start的值
    val request = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)))
    ZkUtils.getLeaderForPartition(zkClient, tp.topic, tp.partition) match {
      case Some(brokerId) =>
        ZkUtils.readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath + "/" + brokerId)._1 match {
          case Some(brokerInfoString) =>
            Json.parseFull(brokerInfoString) match {
              case Some(m) =>
                val brokerInfo = m.asInstanceOf[Map[String, Any]]
                val host = brokerInfo.get("host").get.asInstanceOf[String]
                val port = brokerInfo.get("port").get.asInstanceOf[Int]
                new SimpleConsumer(host, port, 10000, 100000, "getMinOffset")
                  .getOffsetsBefore(request)
                  .partitionErrorAndOffsets(tp)
                  .offsets
                  .head
              case None =>
                throw new BrokerNotAvailableException("Broker id %d does not exist".format(brokerId))
            }
          case None =>
            throw new BrokerNotAvailableException("Broker id %d does not exist".format(brokerId))
        }
    }
  }

  private def getFromOffsets(kafkaParams: Map[String, String], topics: Set[String]): Map[TopicAndPartition, Long] = {
    val zkClient = new ZkClient(kafkaParams("zk_host"))
    zkClient.setZkSerializer(new MyZkSerializer())
    val groupId = kafkaParams("group_id")

    val topics2partitions = ZkUtils.getPartitionsForTopics(zkClient, topics.toSeq)
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    topics2partitions.foreach(topic2Partitions => { // foreach topics
      val topic: String = topic2Partitions._1
      val partitions: Seq[Int] = topic2Partitions._2
      val topicDirs = new ZKGroupTopicDirs(groupId, topic)
      partitions.foreach(partition => { // foreach partitions
        val zkPath = s"${topicDirs.consumerOffsetDir}/$partition"
        ZkUtils.makeSurePersistentPathExists(zkClient, zkPath)
        val untilOffset = zkClient.readData[String](zkPath)

        val tp = TopicAndPartition(topic, partition)
        val minOffset = getMinOffset(tp, zkClient) //minoffset是分区对应的start
        val maxOffset = getMaxOffset(tp, zkClient) //maxoffset 是分区对应的end
        /*
          从zookeeper获取的offset需要与当前topic的earlyoffset/lastoffset进行对比
          如果小于 earlyoffset或者大于lastoffset 则表明zookeeper中的offset不可用
          应更新为 earlyoffset
         */
        var offset = 0l
        if (untilOffset == null || untilOffset.trim == "")
        //第一次 从最新的消息开始消费 或者从最早的消息开始消费
          offset = minOffset
        else if (untilOffset.toLong > maxOffset || untilOffset.toLong < minOffset)
          offset = minOffset
        else
          offset = untilOffset.toLong
        fromOffsets += (tp -> offset)
      })
    })
    fromOffsets
  }
}
