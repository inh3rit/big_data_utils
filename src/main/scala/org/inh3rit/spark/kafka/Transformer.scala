package org.inh3rit.spark.kafka

import com.alibaba.fastjson.JSON
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark._
import org.inh3rit.lang.detector.LangDetector

// import org.elasticsearch.spark._ 使得所有的rdd拥有saveToEs方法

object Transformer {

  case class Trip(id: String, url: String, title: String, content: String, timestamp: String, source_name: String, types: String)

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
    //      .set("spark.es.resource", "index/type") //saveToEs中设置
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

    kafkaStream.foreachRDD(rdd => {
      println(s"-----------------------${rdd.count()}--------------------------")
      //      val records = rdd.map(r => {
      //        println(r._2)
      //        val record = JSON.parseObject(r._2, classOf[Trip])
      //        record
      //      })
      //      records.foreach(record => {
      //        println(record)
      //      })
      val records = rdd.map(r => {
        val record = JSON.parseObject(r._2, classOf[Trip])
        record
        //      }).filter(record => {
        //        var lang = ""
        //        try {
        //          lang = LangDetector.detect(record.content)
        //        } catch {
        //          case e: Exception => // do nothing
        //        }
        //        lang.equals("zh")
      })
      records.saveToEs("test_sent/record", Map("es.batch.size.entries" -> "3000"))
    })

    //把offset更新到zookeeper
    kafkaStream.foreachRDD(rdd => {
      kafkaManager.updateZkOffset(rdd, topics, kafkaParams)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
