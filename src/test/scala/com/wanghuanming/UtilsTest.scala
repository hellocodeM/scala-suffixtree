package com.wanghuanming

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
  * Created by ming on 16-11-17.
  */
@RunWith(classOf[JUnitRunner])
class UtilsTest extends FunSuite {

  test("testSuffixes") {
    val str = "hello"

    assert(Utils.suffixes(str) === "hello".tails.map(_ + "$0").toSet)
  }

}
