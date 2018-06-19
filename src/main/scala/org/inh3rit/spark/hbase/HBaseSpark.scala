package org.inh3rit.spark.hbase

import java.text.SimpleDateFormat

import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.inh3rit.spark.kafka.KafkaManager

object HBaseSpark {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("test_kafka_2_es")
      .setMaster("local")
      // spark streaming config
      .set("spark.streaming.kafka.maxRatePerPartition", "1")
      // spark es config
      .set("es.nodes", "192.168.31.135,192.168.31.136,192.168.31.137,192.168.31.138,192.168.31.139")
      .set("es.port", "9200")
      .set("es.index.auto.create", "true")
    val ssc = new StreamingContext(conf, Seconds(1L))

    val topics = Set("sent-cache-records")
    val kafkaParams = Map(
      "metadata.broker.list" -> "192.168.32.18:9092,192.168.32.19:9092,192.168.32.20:9092",
      "serializer.class" -> "kafka.serializer.StringEncoder",
      //      "auto.offset.reset" -> "smallest", // 从头读取数据
      "fetch.message.max.bytes" -> "22388608",
      "group_id" -> "test_1",
      "zk_host" -> "192.168.32.11:2181,192.168.32.12:2181,192.168.32.13:2181,192.168.32.14:2181,192.168.32.15:2181/kafka"
    )
    val kafkaManager = new KafkaManager()
    val kafkaStream = kafkaManager.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val config = HBaseConfiguration.create
    config.set("hbase.zookeeper.property.clientPort", "2181")
    config.set("hbase.zookeeper.quorum", "192.168.31.135,192.168.31.136,192.168.31.137,192.168.31.138,192.168.31.139")
    config.set(TableInputFormat.INPUT_TABLE,"test_t")
    val connection = ConnectionFactory.createConnection(config)
//    val table = connection.getTable(TableName.valueOf("test_t"))

    kafkaStream.foreachRDD(rdd => {
      rdd.foreachPartition(rdd => {

      })
    })
  }
}
