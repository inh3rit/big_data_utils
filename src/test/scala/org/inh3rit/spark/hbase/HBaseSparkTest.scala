package org.inh3rit.spark.hbase

import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

import scala.collection.JavaConverters._

class HBaseSparkTest extends FunSuite {

  test("hbase write") {
    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val config = HBaseConfiguration.create
    config.set("hbase.zookeeper.property.clientPort", "2181")
    config.set("hbase.zookeeper.quorum", "192.168.31.135,192.168.31.136,192.168.31.137,192.168.31.138,192.168.31.139")
    val connection = ConnectionFactory.createConnection(config)
    val hTable = connection.getTable(TableName.valueOf("test_t"))

    val indataRDD = sc.makeRDD(Array(("5", "jack", "15"), ("6", "Lily", "16"), ("7", "mike", "16"), ("8", "tom", "17")))
    indataRDD.foreachPartition { records =>
      // 举个例子而已，真实的代码根据records来
      val list = new java.util.ArrayList[Put]
      records.foreach(record => {
        val put = new Put(Bytes.toBytes(record._1))
        put.addColumn(Bytes.toBytes("test_f1"), Bytes.toBytes("name"), Bytes.toBytes(record._2))
        put.addColumn(Bytes.toBytes("test_f1"), Bytes.toBytes("age"), Bytes.toBytes(record._3))
        list.add(put)
      })
      // 批量提交
      hTable.put(list)
      // 分区数据写入HBase后关闭连接
      hTable.close()
    }
  }

  // 与上一个配置方式不一样，目前没看出来有什么差别
  //  test("hbase write 1") {
  //    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")
  //    val sc = new SparkContext(sparkConf)
  //    sc.setLogLevel("ERROR")
  //
  //    val tableName = "test_t"
  //    val configuration = HBaseConfiguration.create()
  //    configuration.set("hbase.zookeeper.quorum", "192.168.31.135,192.168.31.136,192.168.31.137,192.168.31.138,192.168.31.139")
  //    configuration.set("hbase.zookeeper.property.clientPort", "2181")
  //    configuration.set(TableInputFormat.INPUT_TABLE, tableName)
  //
  //    val indataRDD = sc.makeRDD(Array(("5", "jack", "15"), ("6", "Lily", "16"), ("7", "mike", "16"), ("8", "tom", "17")))
  //    indataRDD.foreachPartition { records =>
  //      val hTable = new HTable(configuration,tableName)
  //      val puts = records.map(record => {
  //        val put = new Put(record._1.getBytes())
  //        put.addColumn(Bytes.toBytes("test_f1"), Bytes.toBytes("name"), Bytes.toBytes(record._2))
  //        put.addColumn(Bytes.toBytes("test_f1"), Bytes.toBytes("age"), Bytes.toBytes(record._3))
  //        put
  //      })
  //      hTable.put(puts.toList.asJava)
  //    }
  //  }

  test("hbase read") {
    val conf = new SparkConf()
      .setAppName("testHBaseRead")
      .setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val tableName = "test_t"
    val configuration = HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.quorum", "192.168.31.135,192.168.31.136,192.168.31.137,192.168.31.138,192.168.31.139")
    configuration.set("hbase.zookeeper.property.clientPort", "2181")
    configuration.set(TableInputFormat.INPUT_TABLE, tableName)

    val hbaseRDD = sc.newAPIHadoopRDD(configuration, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result])

    val columnRDD = hbaseRDD.map(tuple => {
      val cells = tuple._2.rawCells()
      cells.map(cell => {
        val row = new String(cell.getRowArray, cell.getRowOffset, cell.getRowLength)
        val family = new String(cell.getFamilyArray, cell.getFamilyOffset, cell.getFamilyLength)
        val qualifier = new String(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
        val value = new String(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
        (row, family, qualifier, value)
      })
    })
    columnRDD.foreach(arr => {
      arr.foreach(c => println(c._1, c._2, c._3, c._4))
    })
  }

}

//object HBaseSparkTest {
//  def main(args: Array[String]): Unit = {
//    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")
//    val sc = new SparkContext(sparkConf)
//    sc.setLogLevel("ERROR")
//
//    val tableName = "test_t"
//    val configuration = HBaseConfiguration.create()
//    configuration.set("hbase.zookeeper.quorum", "192.168.31.135,192.168.31.136,192.168.31.137,192.168.31.138,192.168.31.139")
//    configuration.set("hbase.zookeeper.property.clientPort", "2181")
//    configuration.set(TableInputFormat.INPUT_TABLE, tableName)
//
//    val indataRDD = sc.makeRDD(Array(("5", "jack", "15"), ("6", "Lily", "16"), ("7", "mike", "16"), ("8", "tom", "17")))
//    indataRDD.foreachPartition { records =>
//      val hTable = new HTable(configuration, tableName)
//      val puts = records.map(record => {
//        val put = new Put(record._1.getBytes())
//        put.addColumn(Bytes.toBytes("test_f1"), Bytes.toBytes("name"), Bytes.toBytes(record._2))
//        put.addColumn(Bytes.toBytes("test_f1"), Bytes.toBytes("age"), Bytes.toBytes(record._3))
//        put
//      })
//      hTable.put(puts.toList.asJava)
//    }
//  }
//}
