package org.inh3rit.spark.kafka

import java.text.SimpleDateFormat

import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransformerTest {

  case class Trip(id: String, url: String, title: String, content: String, source_name: String, types: String, timestamp: String)

  //  def main(args: Array[String]): Unit = {
  //    val conf = new SparkConf()
  //      .setAppName("test_es_spark")
  //      .setMaster("local")
  //      .set("es.nodes", "192.168.31.135,192.168.31.136,192.168.31.137,192.168.31.138,192.168.31.139")
  //      .set("es.port", "9200")
  //      .set("es.index.auto.create", "true")
  //    val sc = SparkSession.builder()
  //      .config(conf)
  //      .getOrCreate()
  //      .sparkContext
  ////    val upcomingTrip = Trip("id1", null,null,null,null,null,null)
  //    val upcomingTrip = Trip("id3", "url1", "title1", "content1", "source_name1", "types1", "2018-04-01 12:00:00")
  //    val rdd = sc.makeRDD(Seq(
  //      upcomingTrip
  //    ))
  ////    rdd.saveToEs("test_sent/record")
  //    rdd.saveToEs("test_sent/record", Map("_id"->"id"))
  //
  //    sc.stop()
  //  }


  val configuration = HBaseConfiguration
          .create()
    configuration.set("hbase.zookeeper.quorum", "192.168.31.135,192.168.31.136,192.168.31.137,192.168.31.138,192.168.31.139")
    configuration.set("hbase.zookeeper.property.clientPort", "2181")
          //设置zookeeper连接端口，默认2181
    val connection = ConnectionFactory.createConnection(configuration)
  val admin = connection.getAdmin
  val table = connection.getTable(TableName.valueOf("test_sent"))

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

    //    kafkaStream.foreachRDD(rdd => {
    //      println(s"-----------------------${rdd.count()}--------------------------")
    //      rdd.foreachPartition(partitionRecords => {
    //        partitionRecords.foreach(s => {
    //          println(s)
    //
    //
    //        })
    //      })
    //    })

    //把offset更新到zookeeper
    //    kafkaStream.foreachRDD(rdd => {
    //      kafkaManager.updateZkOffset(rdd, topics, kafkaParams)
    //    })

    kafkaStream.foreachRDD(rdd => {
      rdd.foreachPartition(pa => {
        pa.foreach(p => {
          val get = new Get(Bytes.toBytes(p._1))
          val resultGet = table.get(get)
          resultGet.advance() match {
            case true => {
              //              val accountNum = Bytes.toInt(resultGet.getValue(Bytes.toBytes("i"), Bytes.toBytes("accountNum")))
              //              val amountNum = Bytes.toInt(resultGet.getValue(Bytes.toBytes("i"), Bytes.toBytes("amountNum")))
              //              insertHbase(p._1, p._2._1 + accountNum, p._2._2 + amountNum)
              resultGet.getValue(Bytes.toBytes(""), Bytes.toBytes(""))
            }
            case _ =>
          }
        })
      })
    })


    ssc.start()
    ssc.awaitTermination()
  }

  //创建hbase表 String* 创建多个列簇
  def createTable(table: Table, colFamily: String*): Unit = {
    if (!admin.tableExists(TableName.valueOf("order_static"))) {
      val descriptor = new HTableDescriptor(TableName.valueOf("order_static"))
      //foreach创建多个列簇
      colFamily.foreach(x => descriptor.addFamily(new HColumnDescriptor(x)))
      admin.createTable(descriptor)
      admin.close()
    }
  }

  //向hbase表中插入数据
  def insertHbase(productId: String, accountNum: Int, amountNum: Int): Unit = {
    val put = new Put(Bytes.toBytes(productId))
    put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("accountNum"), Bytes.toBytes(accountNum))
    put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("amountNum"), Bytes.toBytes(amountNum))
  }


}

//https://blog.csdn.net/xiushuiguande/article/details/79922776