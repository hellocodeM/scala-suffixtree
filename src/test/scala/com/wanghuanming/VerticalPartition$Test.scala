package com.wanghuanming

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
  * Created by ming on 16-11-15.
  */
@RunWith(classOf[JUnitRunner])
class VerticalPartition$Test extends FunSuite {

  test("testPartition") {
    val s = "hello"
    val alphabet = s.toCharArray
    val res = VerticalPartition.partition(s, alphabet, 2)

    res.foreach(x => println(x.mkString(",")))
  }

}
