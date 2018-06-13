package org.inh3rit.spark.kafka

class ScalaTest extends org.scalatest.FunSuite {

  test("test private variables") {
    val aaa = 1
    val a = {
      //      println(aaa) // ERROR:wrong forward reference
      val aaa = 2
      aaa
    }
    println(a)
  }
}
