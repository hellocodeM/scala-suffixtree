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
    val str = "test"
    val label = "0"

    val expected = Array("0:test$", "0:est$", "0:st$", "0:t$").sorted
    assert(Utils.suffixesWithLabel(label, str) === expected)
    assert(Utils.suffixes(str) === expected)
  }

}
