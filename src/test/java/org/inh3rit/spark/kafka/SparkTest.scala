package org.inh3rit.spark.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.FunSuite

class SparkTest extends FunSuite{

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("test_offset")
      .setMaster("local")
      .set("spark.streaming.kafka.maxRatePerPartition", "1")
    val ssc = new StreamingContext(conf, Seconds(1L))

    val topics = Set("sent-cache-records")
    val kafkaParams = Map(
      "metadata.broker.list" -> "192.168.32.18:9092,192.168.32.19:9092,192.168.32.20:9092",
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "auto.offset.reset" -> "smallest", // 从头读取数据
      "fetch.message.max.bytes" -> "22388608",
      "group_id" -> "test_1",
      "zk_host" -> "192.168.32.11:2181,192.168.32.12:2181,192.168.32.13:2181,192.168.32.14:2181,192.168.32.15:2181/kafka"
    )
    val kafkaManager = new KafkaManager()
    val kafkaStream = kafkaManager.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    kafkaStream.foreachRDD(rdd => {
      println(s"-----------------------${rdd.count()}--------------------------")
      rdd.foreach(r => {
        println(r._2)
      })
    })

    //把offset更新到zookeeper
    kafkaStream.foreachRDD(rdd => {
      /*
        把RDD转成HasOffsetRanges类型（KafkaRDD extends HasOffsetRanges）
        OffsetRange 说明：Represents a range of offsets from a single Kafka TopicAndPartition.
        OffsetRange 说明： Instances of this class can be created with `OffsetRange.create()`.
       */
      kafkaManager.updateZkOffset(rdd, topics, kafkaParams)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
