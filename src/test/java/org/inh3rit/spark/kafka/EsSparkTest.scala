package org.inh3rit.spark.kafka

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._

object EsSparkTest {

  case class Trip(id: String, url: String, title: String, content: String, source_name: String, types: String, timestamp: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("test_es_spark")
      .setMaster("local")
      .set("es.nodes", "192.168.31.135,192.168.31.136,192.168.31.137,192.168.31.138,192.168.31.139")
      .set("es.port", "9200")
      .set("es.index.auto.create", "true")
    val sc = SparkSession.builder()
      .config(conf)
      .getOrCreate()
      .sparkContext
//    val upcomingTrip = Trip("id1", null,null,null,null,null,null)
    val upcomingTrip = Trip("id3", "url1", "title1", "content1", "source_name1", "types1", "2018-04-01 12:00:00")
    val rdd = sc.makeRDD(Seq(
      upcomingTrip
    ))
//    rdd.saveToEs("test_sent/record")
    rdd.saveToEs("test_sent/record", Map("_id"->"id"))

    sc.stop()
  }

}
