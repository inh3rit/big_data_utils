package org.inh3rit.spark.kafka

import org.apache.spark.sql.SparkSession

class ScalaTest extends org.scalatest.FunSuite {

  test("private variables") {
    val aaa = 1
    val a = {
      //      println(aaa) // ERROR:wrong forward reference
      val aaa = 2
      aaa
    }
    println(a)
  }

  test("mkString"){
    val arr = Array(80, 21, 5900, 22, 3306, 1433, 1521, 5432, 5000, 27017, 3389, 445, 139)
    val str = arr.mkString(",")
    print(str)

  }

  test("wordCount") {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    val lines = spark.readStream
      .format("socket")
      .option("host", "spark://172.16.116.5:7077")
      .option("port", 9999)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }

  test("print") {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local")
      .getOrCreate()

    val sc = spark.sparkContext
    val rdd = sc.parallelize(List(1,2,3,5))
    rdd.map(_ + 1).foreach(println)

    spark.close()

  }
}
