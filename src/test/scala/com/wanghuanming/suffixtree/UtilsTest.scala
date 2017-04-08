package com.wanghuanming.suffixtree

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.util.Random

/**
  * Created by ming on 16-11-17.
  */
@RunWith(classOf[JUnitRunner])
class UtilsTest extends FunSuite {

  test("suffixes") {
    val str = "test"
    val label = "0"

    val expected = Array("0:test$", "0:est$", "0:st$", "0:t$").sorted
    assert(Utils.suffixesWithLabel(label, str) === expected)
    assert(Utils.suffixes(str) === expected)
  }

  test("getAlphabet") {
    val strs = Iterable(RangeSubString("hello"))
    val alphabet = Utils.getAlphabet(strs)

    assert("helo".sorted === alphabet.sorted)
  }

  test("genTerminal") {
    for (i <- 1 to 100) {
      val s = Random.nextString(200)
      val terminal = Utils.genTerminal(Utils.getAlphabet(s))
      assert(!s.contains(terminal))
    }
  }
}
