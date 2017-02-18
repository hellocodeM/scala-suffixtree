package com.wanghuanming

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
  * Created by ming on 16-11-17.
  */
@RunWith(classOf[JUnitRunner])
class UtilsTest extends FunSuite {
  val conf = new SparkConf().setMaster("local[4]").setAppName("UtilsTest")
  val sc = new SparkContext(conf)

  test("testSuffixes") {
    val str = "test"
    val label = "0"

    val expected = Array("0:test$", "0:est$", "0:st$", "0:t$").sorted
    assert(Utils.suffixesWithLabel(label, str) === expected)
    assert(Utils.suffixes(str) === expected)
  }

  test("readFromHdfs") {
    Utils.readAllStringFromFile("src/test/resources/exset/ex0").foreach(println)
    println()
    Utils.readAllStringFromFile(sc, "src/test/resources/exset/ex0").foreach(println)
  }

  /*test("writeToHdfs") {
    val arr = Array("adasdsa", "sadsadsa", "Sads")
    Utils.writeLeafInfoToFile("src/test/resources/result/part-", arr)
  }*/
}
