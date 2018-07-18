package org.inh3rit.spark.elasticsearch

import java.io.File
import java.text.SimpleDateFormat

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

import scala.io.Source


class elasticsearchTest extends FunSuite {

  test("spark sql support") {
    val spark = SparkSession
      .builder()
      .appName("SparkSqlSupport")
      .master("local[4]")
      .config("es.nodes", "192.168.226.53")
      .config("es.port", "9200")
      .config("es.index.auto.create", "true")
      .config("es.read.metadata", "true")
      .config("spark.executor.logs.rolling.strategy", "time")
      .config("spark.executor.logs.rolling.maxRetainedFiles", "1")
      .config("spark.executor.logs.rolling.enableCompression", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "1024M")
      .config("spark.kryoserializer.buffer", "1024k")
      .config("es.scroll.size", "20000")
      .config("es.read.metadata.field", "metadata")
      .config("spark.dynamicAllocation.enabled", "false")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val options = Map("pushdown" -> "true", "double.filtering" -> "false")
    spark.sqlContext.read
      .format("org.elasticsearch.spark.sql")
      .options(options)
      .load("apt_custom_log2018-07-13/basic")
      .createTempView("basic")

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val starttime = sdf.parse("2018-07-13 00:00:00").getTime / 1000
    val endtime = sdf.parse("2018-07-13 01:00:00").getTime / 1000

    println(starttime, endtime)

    val start = System.currentTimeMillis()

    // customize udf(user defined function) ①
    def myUDF(timestamp: Long): String = {
      return sdf.format(timestamp)
    }

    spark.udf.register("myUDF", myUDF _) // ②
    //    spark.udf.register("myUDF", (timestamp: Long) => sdf.format(timestamp * 1000))

    spark.sql(s"select srcip, myUDF(starttime),  COUNT(srcip), SUM(downflow), SUM(upflow) from basic " + // ③
      s"where starttime > $starttime and starttime < $endtime " +
      s"group by srcip")
      .show()

    //    spark.sql(s"select srcip, downflow, upflow from basic " +
    //      s"where starttime > $starttime and starttime < $endtime ")
    //      .groupBy("srcip")
    //      .agg(("srcip", "count"), ("downflow", "sum"), ("upflow", "sum"))
    //      .show()

    //    spark.sql(s"select srcip, starttime from basic " +
    //      s"where starttime > $starttime and starttime < $endtime ")
    //      .groupBy("srcip")
    //      .agg(("starttime", "collect_set"))
    //      .show()

    //    spark.sql(s"select srcip, collect_set(starttime) from basic " +
    //      s"where starttime > $starttime and starttime < $endtime " +
    //      s"group by srcip")
    //      .show()

    val end = System.currentTimeMillis()
    println(s"spend ${end - start}")

    spark.close()
  }

  test("json parse") {
    val sb = new StringBuilder
    Source.fromFile("src/test/resources/test.json").getLines().foreach(line => sb.append(line))
    val jo = JSON.parseObject(sb.toString())
    jo.forEach((k, v) => println(k, v))
  }

  test("print") {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val starttime = sdf.parse("2018-07-13 00:00:00").getTime
    val endtime = sdf.parse("2018-07-13 04:00:00").getTime
    println(starttime, endtime)
  }
}
